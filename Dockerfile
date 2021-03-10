FROM ekidd/rust-musl-builder:stable as builder

RUN USER=root cargo new --bin nun-db
WORKDIR ./nun-db

COPY ./Cargo.toml ./Cargo.toml
RUN cargo build --release
RUN rm src/*.rs

ADD . ./

RUN rm ./target/x86_64-unknown-linux-musl/release/deps/nun*
RUN cargo build --release

FROM alpine:3.12.4

RUN apk add libressl-dev
COPY --from=builder /home/rust/src/nun-db/target/x86_64-unknown-linux-musl/release/nun-db /usr/bin/nun-db

CMD ["sh" , "-c", "nun-db -u ${NUN_USER} -p ${NUN_PWD} start --http-address ${NUN_HTTP_ADDR} --tcp-address ${NUN_TCP_ADDR} --ws-address ${NUN_WS_ADDR} "]
