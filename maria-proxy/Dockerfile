FROM alpine:3.19
RUN apk add --no-cache socat
CMD socat TCP-LISTEN:3306,fork TCP:18.218.110.222:3306