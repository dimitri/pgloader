# Multi-stage build for pgloader Clojure v4
FROM clojure:temurin-21-jammy AS builder

WORKDIR /app

# Copy project files
COPY deps.edn .
COPY build.clj .
COPY src/ src/
COPY resources/ resources/

# Build the uberjar
RUN clojure -T:build uber

# Final stage - runtime image
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

# Copy the built jar
COPY --from=builder /app/target/pgloader-v4-0.1.0-SNAPSHOT.jar /app/pgloader.jar

# Create a non-root user
RUN groupadd --system pgloader \
  && useradd --system --gid pgloader pgloader \
  && mkdir -p /tmp/pgloader \
  && chown -R pgloader:pgloader /app \
  && chown -R pgloader:pgloader /tmp/pgloader

USER pgloader

ENTRYPOINT ["java", "-jar", "/app/pgloader.jar"]
CMD ["--help"]
