FROM maven AS build_img
COPY . /usr/src/app
WORKDIR /usr/src/app
RUN mvn clean package


FROM eclipse-temurin:11
COPY --from=build_img /usr/src/app/bin /javabin
WORKDIR /javabin
ENTRYPOINT [ "java", "-jar", "/javabin/POCDriver.jar" ]
