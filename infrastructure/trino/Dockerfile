# Trino 483 does not reuse the catalog OAuth2 session for REST catalogs using
# session type NONE. Backport the exact upstream fix merged as trinodb/trino#30816
# while preserving the released 483 runtime and connector surface.
ARG TRINO_BASE_IMAGE="trinodb/trino:483@sha256:db58cc93e593a2706553745f276bb119c9810e69918be56ecde088ba7ccb0534"
FROM ${TRINO_BASE_IMAGE}

USER root

COPY patches/30816/ /tmp/trino-30816/

RUN mkdir -p /tmp/trino-30816-classes \
    && javac \
        -cp '/usr/lib/trino/plugin/iceberg/*' \
        -d /tmp/trino-30816-classes \
        $(find /tmp/trino-30816 -type f -name '*.java' -print) \
    && jar --update \
        --file /usr/lib/trino/plugin/iceberg/io.trino_trino-iceberg-483.jar \
        -C /tmp/trino-30816-classes . \
    && jar tf /usr/lib/trino/plugin/iceberg/io.trino_trino-iceberg-483.jar \
        | grep -q 'io/trino/plugin/iceberg/catalog/rest/SharedSessionOAuth2Manager.class' \
    && rm -rf /tmp/trino-30816 /tmp/trino-30816-classes

USER trino:trino
