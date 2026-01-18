import os
import socket
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from pyspark.sql import SparkSession

# Default output location for the car models dim table
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "s3a://spark/data/dims/car_models")

# Mid-project car paths
CARS_INPUT_PATH = os.getenv("CARS_INPUT_PATH", "s3a://spark/data/dims/cars")
CAR_MODELS_PATH = os.getenv("CAR_MODELS_PATH", "s3a://spark/data/dims/car_models")
CAR_COLORS_PATH = os.getenv("CAR_COLORS_PATH", "s3a://spark/data/dims/car_colors")

# Kafka / Kafdrop defaults
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensors-sample")
KAFKA_ENRICHED_TOPIC = os.getenv("KAFKA_ENRICHED_TOPIC", "samples-enriched")
KAFKA_ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "alert-data")
KAFDROP_URL = os.getenv("KAFDROP_URL", "http://localhost:9003")

# Spark connector packages
SPARK_KAFKA_PACKAGE = os.getenv(
	"SPARK_KAFKA_PACKAGE",
	"org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
)

# DataGenerator behavior
PREVIEW_COUNT = int(os.getenv("PREVIEW_COUNT", "3"))
REQUIRE_KAFKA = os.getenv("REQUIRE_KAFKA", "0").strip() == "1"

# Kafka producer knobs (keep conservative defaults to avoid long hangs)
KAFKA_CONNECT_ATTEMPTS = int(os.getenv("KAFKA_CONNECT_ATTEMPTS", "20"))
KAFKA_CONNECT_DELAY_SECONDS = float(os.getenv("KAFKA_CONNECT_DELAY_SECONDS", "1"))
KAFKA_RECONNECT_DELAY_SECONDS = float(os.getenv("KAFKA_RECONNECT_DELAY_SECONDS", "5"))
KAFKA_MAX_BLOCK_MS = int(os.getenv("KAFKA_MAX_BLOCK_MS", "10000"))
KAFKA_REQUEST_TIMEOUT_MS = int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "10000"))

# Prevent AWS SDK from trying EC2 instance metadata (can hang in containers)
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")


def _host_from_endpoint(endpoint: str) -> str:
	if "://" in endpoint:
		return urlparse(endpoint).hostname or endpoint
	return endpoint.split(":", 1)[0]


def _resolves(hostname: str) -> bool:
	try:
		socket.getaddrinfo(hostname, None)
		return True
	except OSError:
		return False


def pick_kafka_bootstrap() -> str:
	"""Pick a Kafka bootstrap server depending on runtime environment."""

	env_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
	if env_bootstrap:
		# If user provided a bootstrap list, sanity-check that at least one hostname resolves.
		candidates = [s.strip() for s in env_bootstrap.split(",") if s.strip()]
		resolvable = []
		for c in candidates:
			host = c
			if "://" in host:
				host = urlparse(host).hostname or host
			# strip port if present
			host = host.rsplit(":", 1)[0]
			if _resolves(host):
				resolvable.append(c)
		if resolvable:
			return ",".join(resolvable)
		print(
			f"[config] WARN: KAFKA_BOOTSTRAP_SERVERS has no resolvable hosts: {env_bootstrap!r}. "
			"Falling back to auto-detection.",
			flush=True,
		)

	# If running inside the docker-compose network, this is correct.
	if _resolves("course-kafka"):
		return "course-kafka:9092"

	# If running in a VS Code devcontainer outside the compose network.
	if _resolves("host.docker.internal"):
		# Prefer the devcontainer-friendly listener.
		return "host.docker.internal:29092"

	# Host fallback: use the host-friendly listener.
	return "localhost:29093"


def create_kafka_producer(bootstrap_servers: str | None = None):
	"""Create a kafka-python KafkaProducer.

	Returns:
		KafkaProducer instance.

	Raises:
		RuntimeError if REQUIRE_KAFKA=1 and broker is not reachable.
	"""

	# Import lazily so non-Kafka scripts that import config still work.
	from kafka import KafkaProducer

	bootstrap = bootstrap_servers or pick_kafka_bootstrap()

	last_exc: Exception | None = None
	for _ in range(max(1, KAFKA_CONNECT_ATTEMPTS)):
		try:
			return KafkaProducer(
				bootstrap_servers=[s.strip() for s in bootstrap.split(",") if s.strip()],
				value_serializer=lambda v: v.encode("utf-8"),
				acks=1,
				retries=5,
				request_timeout_ms=KAFKA_REQUEST_TIMEOUT_MS,
				max_block_ms=KAFKA_MAX_BLOCK_MS,
			)
		except Exception as exc:
			last_exc = exc
			import time
			time.sleep(KAFKA_CONNECT_DELAY_SECONDS)

	msg = (
		"[config] WARNING: Kafka not reachable. "
		"If you are not attached to the docker-compose network, either:\n"
		"- Attach VS Code to the running `dev_env` container (then `course-kafka:9092` works), or\n"
		"- Configure Kafka to advertise a host-reachable listener, or\n"
		"- Set KAFKA_BOOTSTRAP_SERVERS to a reachable broker."
	)

	if REQUIRE_KAFKA:
		raise RuntimeError(msg) from last_exc

	print(msg, flush=True)
	return None


def pick_s3a_endpoint() -> str:
	"""Pick an S3A endpoint for MinIO.

	This repo's MinIO S3 port is exposed on host port 9001.
	- If you're in the docker-compose network: use http://minio:9000
	- If you're in a VS Code devcontainer not on that network: use http://host.docker.internal:9001
	"""

	# Highest priority: explicit override
	env_endpoint = os.getenv("S3A_ENDPOINT")
	if env_endpoint:
		return env_endpoint

	# In the compose network, service-to-service uses the container port.
	if _resolves("minio"):
		return "http://minio:9000"

	def _looks_like_console(url: str) -> bool:
		# Console typically responds 200 + text/html + Server: MinIO Console
		try:
			req = Request(url, method="GET")
			with urlopen(req, timeout=2) as resp:
				server = (resp.headers.get("Server") or "").lower()
				ctype = (resp.headers.get("Content-Type") or "").lower()
				return ("minio console" in server) or ("text/html" in ctype)
		except Exception:
			return False

	# Devcontainer / not on compose network: prefer the host-published S3 port (9001).
	# Some environments also publish the console on 9002; avoid using that for S3A.
	if _resolves("host.docker.internal"):
		s3 = "http://host.docker.internal:9001"
		console = "http://host.docker.internal:9002"
		if _looks_like_console(console):
			return s3
		# If 9002 isn't a console (custom mapping), allow using it.
		return console

	# Host machine fallback
	if _looks_like_console("http://localhost:9002"):
		return "http://localhost:9001"
	return "http://localhost:9002"


def create_spark(app_name: str = "mid_project_car", spark_packages: str | None = None) -> SparkSession:
	endpoint = pick_s3a_endpoint()
	region = os.getenv("AWS_REGION", "us-east-1")
	access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
	secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

	print(f"[config] Using S3A endpoint: {endpoint}", flush=True)

	builder = (
		SparkSession.builder.master("local[*]")
		.appName(app_name)
		.config("spark.ui.enabled", "false")
		.config("spark.sql.shuffle.partitions", "1")
		.config("spark.default.parallelism", "1")
	)

	if spark_packages:
		builder = builder.config("spark.jars.packages", spark_packages)

	spark = (
		builder
		# S3A / MinIO
		.config("spark.hadoop.fs.s3a.endpoint", endpoint)
		.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
		.config("spark.hadoop.fs.s3a.endpoint.region", region)
		.config("spark.hadoop.fs.s3a.path.style.access", "true")
		.config("spark.hadoop.fs.s3a.access.key", access_key)
		.config("spark.hadoop.fs.s3a.secret.key", secret_key)
		.config(
			"spark.hadoop.fs.s3a.aws.credentials.provider",
			"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
		)
		# Avoid slow/fragile metadata lookups in local/devcontainer contexts.
		.config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
		.config("spark.hadoop.fs.s3a.connection.timeout", "5000")
		.config("spark.hadoop.fs.s3a.socket.timeout", "5000")
		.getOrCreate()
	)

	if endpoint.startswith("http://"):
		# This must be a Hadoop config (spark.hadoop.*), not just Spark SQL conf.
		spark.sparkContext._jsc.hadoopConfiguration().set(
			"fs.s3a.connection.ssl.enabled", "false"
		)

	spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

	# Fail-fast or warn if the endpoint hostname can't resolve from this runtime
	preflight_mode = os.getenv("S3A_PREFLIGHT_DNS", "warn").lower()
	if preflight_mode != "off":
		host = _host_from_endpoint(endpoint)
		if not _resolves(host):
			msg = (
				"S3A endpoint hostname is not resolvable from this runtime: "
				f"endpoint={endpoint!r}. Set S3A_ENDPOINT to a reachable host:port."
			)
			if preflight_mode == "strict":
				raise RuntimeError(msg)
			print(f"[config] WARN: {msg}", flush=True)

	return spark


# Lazily created Spark session. Some jobs (e.g., Kafka streaming) must start Spark
# with additional packages; those cannot be added after Spark is already running.
_spark: SparkSession | None = None
_spark_packages: str | None = None


def get_spark(app_name: str = "mid_project_car", spark_packages: str | None = None) -> SparkSession:
	global _spark, _spark_packages

	if _spark is None:
		_spark_packages = spark_packages
		_spark = create_spark(app_name=app_name, spark_packages=spark_packages)
		return _spark

	# If a caller requests packages that differ from the currently running Spark session,
	# restart the session so the new packages take effect.
	if spark_packages and spark_packages != _spark_packages:
		try:
			_spark.stop()
		except Exception:
			pass
		_spark_packages = spark_packages
		_spark = create_spark(app_name=app_name, spark_packages=spark_packages)
		return _spark

	return _spark


