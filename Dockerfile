FROM rust:1.38.0

WORKDIR /usr/src/freira-db
COPY ./ .

RUN cargo install

CMD ["freira-db"]
EXPOSE 3012
