FROM openjdk:8-alpine

MAINTAINER roi@logz.io

RUN apk add --no-cache bash

ADD go.sh /root

ADD jmx2graphite-1.2.4-javaagent.jar /root

RUN chmod a+x /root/go.sh

CMD /root/go.sh

ADD packages/ /root
