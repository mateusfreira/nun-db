FROM rust:1.38.0

WORKDIR /usr/src/nun-db
COPY ./ .

RUN cargo install

CMD ["nun-db"]
EXPOSE 3012
