# Multi-stage build for pgloader Clojure v4
#
# Use BUILDPLATFORM for the builder stage so that the Clojure compilation
# always runs on the native host architecture (linux/amd64 on GitHub Actions),
# producing a platform-independent JAR.  The runtime stage uses TARGETPLATFORM,
# so the correct eclipse-temurin arm64/amd64 layer is pulled from the registry.
FROM --platform=$BUILDPLATFORM clojure:temurin-21-jammy AS builder

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
COPY --from=builder /app/target/pgloader.jar /app/pgloader.jar

# Create a non-root user
RUN groupadd --system pgloader \
  && useradd --system --gid pgloader pgloader \
  && mkdir -p /tmp/pgloader \
  && chown -R pgloader:pgloader /app \
  && chown -R pgloader:pgloader /tmp/pgloader

USER pgloader

ENTRYPOINT ["java", "-jar", "/app/pgloader.jar"]
CMD ["--help"]
