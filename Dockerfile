FROM maven

COPY . /usr/src/app
WORKDIR /usr/src/app

RUN mvn clean package

ENTRYPOINT [ "java", "-jar", "bin/POCDriver.jar" ]
