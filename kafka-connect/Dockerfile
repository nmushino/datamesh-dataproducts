# Debezium PostgreSQL コネクタ (Outbox イベントルーティング用) を組み込んだ
# Kafka Connect イメージ。Strimzi (AMQ Streams) の KafkaConnect リソースは
# 独自のエントリポイント/レイアウトを前提とするため、Strimzi 自身が使っている
# ベースイメージにプラグインを追加する形でビルドする
# (registry.redhat.io/amq-streams/kafka-41-rhel9 = このクラスタで稼働中の
# Kafka 4.1.0 用 AMQ Streams イメージと同一)。
#
# debezium.version はアプリ側 (quarkusdroneshop-inventory/pom.xml,
# quarkusdroneshop-counter/pom.xml) の debezium-quarkus-outbox と揃えてある。
FROM registry.redhat.io/amq-streams/kafka-41-rhel9@sha256:8053e93f202c5b2523709c0bba26a5fdf0f9fe7e3ce38b4b2a3c4943a446eaba

USER root:root

ARG DEBEZIUM_VERSION=3.5.2.Final

RUN mkdir -p /opt/kafka/plugins/debezium-connector-postgres \
    && curl -fsSL -o /tmp/debezium-postgres.tar.gz \
       "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz" \
    && tar xzf /tmp/debezium-postgres.tar.gz -C /opt/kafka/plugins/debezium-connector-postgres --strip-components=1 \
    && rm -f /tmp/debezium-postgres.tar.gz

USER 1001
