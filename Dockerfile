ARG JAVA_VERSION
FROM eclipse-temurin:${JAVA_VERSION:-8}
ARG SCALA_VERSION
ENV SCALA_VERSION ${SCALA_VERSION:-2.12.11}
ARG SBT_VERSION
ENV SBT_VERSION ${SBT_VERSION:-1.5.8}
ENV USER_ID 1001
ENV GROUP_ID 1001
RUN apt-get update && \
  apt-get install bash rpm -y && \
  rm -rf /var/lib/apt/lists/*
# Install sbt
RUN \
  curl -fsL "https://github.com/sbt/sbt/releases/download/v$SBT_VERSION/sbt-$SBT_VERSION.tgz" | tar xfz - -C /usr/share && \
  chown -R root:root /usr/share/sbt && \
  chmod -R 755 /usr/share/sbt && \
  ln -s /usr/share/sbt/bin/sbt /usr/local/bin/sbt
# # Install Scala
RUN \
  case $SCALA_VERSION in \
    "3"*) URL=https://github.com/lampepfl/dotty/releases/download/$SCALA_VERSION/scala3-$SCALA_VERSION.tar.gz SCALA_DIR=/usr/share/scala3-$SCALA_VERSION ;; \
    *) URL=https://downloads.typesafe.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.tgz SCALA_DIR=/usr/share/scala-$SCALA_VERSION ;; \
  esac && \
  curl -fsL $URL | tar xfz - -C /usr/share && \
  mv $SCALA_DIR /usr/share/scala && \
  chown -R root:root /usr/share/scala && \
  chmod -R 755 /usr/share/scala && \
  ln -s /usr/share/scala/bin/* /usr/local/bin && \
  case $SCALA_VERSION in \
    "3"*) echo "@main def main = println(util.Properties.versionMsg)" > test.scala ;; \
    *) echo "println(util.Properties.versionMsg)" > test.scala ;; \
  esac && \
  scala -nocompdaemon test.scala && rm test.scala
# # Add and use user sbtuser
RUN groupadd --gid $GROUP_ID sbtuser && useradd -m --gid $GROUP_ID --uid $USER_ID sbtuser --shell /bin/bash
USER sbtuser
# # Switch working directory
WORKDIR /home/sbtuser
# # Prepare sbt (warm cache)
RUN \
  sbt sbtVersion && \
  mkdir -p project && \
  echo "scalaVersion := \"${SCALA_VERSION}\"" > build.sbt && \
  echo "sbt.version=${SBT_VERSION}" > project/build.properties && \
  echo "// force sbt compiler-bridge download" > project/Dependencies.scala && \
  echo "case object Temp" > Temp.scala && \
  sbt compile && \
  rm -r project && rm build.sbt && rm Temp.scala && rm -r target
USER root
# # Link everything into root as well
# # This allows users of this container to choose, whether they want to run the container as sbtuser (non-root) or as root
RUN \
  ln -s /home/sbtuser/.cache /root/.cache && \
  ln -s /home/sbtuser/.sbt /root/.sbt && \
  if [ -d "/home/sbtuser/.ivy2" ]; then ln -s /home/sbtuser/.ivy2 /root/.ivy2; fi
# # Switch working directory back to root
# ## Users wanting to use this container as non-root should combine the two following arguments
# ## -u sbtuser
# ## -w /home/sbtuser
WORKDIR /root
# # Add zscaler cert
# RUN keytool -import -alias zscaler -file /root/ZscalerRootCA.crt -keystore $JAVA_HOME/lib/security/cacerts -storepass changeit
CMD [ "/bin/bash" ]
COPY . /hydra