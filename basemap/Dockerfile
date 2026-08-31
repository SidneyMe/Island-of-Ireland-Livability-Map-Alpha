FROM maven:3.9.9-eclipse-temurin-21 AS build

ARG PROTOMAPS_BASEMAPS_COMMIT
RUN apt-get update \
    && apt-get install --yes --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/protomaps/basemaps.git /src/basemaps \
    && git -C /src/basemaps checkout --detach "${PROTOMAPS_BASEMAPS_COMMIT}"
RUN cd /src/basemaps/tiles && mvn --batch-mode --no-transfer-progress -DskipTests clean package

FROM eclipse-temurin:21-jre
WORKDIR /work
COPY --from=build /src/basemaps/tiles/target/protomaps-basemap-HEAD-with-deps.jar /opt/protomaps-basemap.jar
ENTRYPOINT ["java", "-jar", "/opt/protomaps-basemap.jar"]
