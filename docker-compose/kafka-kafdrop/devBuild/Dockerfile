FROM openjdk:17-jdk-oraclelinux7

ADD kafdrop.sh /
ADD kafdrop*tar.gz /
ADD externalserdes.jar /

RUN chmod +x /kafdrop.sh

ENTRYPOINT ["/kafdrop.sh"]
