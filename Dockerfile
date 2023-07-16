FROM debian:buster-slim AS chef 
RUN apt-get update && \
    export DEBIAN_FRONTEND=noninteractive && \
    apt-get install -yq \
    build-essential \
    cmake \
    clang \ 
    curl \
    protobuf-compiler
ENV RUSTUP_HOME=/opt/rust/rustup \
    PATH=/home/root/.cargo/bin:/opt/rust/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
RUN curl https://sh.rustup.rs -sSf | \
    env CARGO_HOME=/opt/rust/cargo \
    sh -s -- -y --default-toolchain stable --profile minimal --no-modify-path && \
    env CARGO_HOME=/opt/rust/cargo \
    rustup component add rustfmt
RUN env CARGO_HOME=/opt/rust/cargo cargo install cargo-chef && \
    rm -rf /opt/rust/cargo/registry/
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml .
COPY Cargo.lock .
COPY main/ main/
COPY src/ src/
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder 
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY Cargo.toml .
COPY Cargo.lock .
COPY main/ main/
COPY src/ src/
RUN cargo build --release
RUN cargo build --manifest-path=main/crates/install/Cargo.toml --release

FROM debian:buster-slim AS runtime

COPY --from=builder /app/target/release/stalwart-jmap /usr/local/bin/stalwart-jmap
COPY --from=builder /app/main/target/release/stalwart-install /usr/local/bin/stalwart-install
RUN echo "#\!/bin/sh\n\n/usr/local/bin/stalwart-install -c jmap -p /opt/stalwart-jmap -d" > /usr/local/bin/configure.sh && \
    chmod +x /usr/local/bin/configure.sh
RUN useradd stalwart-jmap -s /sbin/nologin -M
RUN mkdir -p /opt/stalwart-jmap
RUN chown stalwart-jmap:stalwart-jmap /opt/stalwart-jmap

VOLUME [ "/opt/stalwart-jmap" ]

ENTRYPOINT ["/usr/local/bin/stalwart-jmap", "--config", "/opt/stalwart-jmap/etc/config.toml"]
