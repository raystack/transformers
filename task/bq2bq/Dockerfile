ARG VERSION
FROM docker.io/odpf/optimus-task-bq2bq-executor:${VERSION}

ARG OPTIMUS_RELEASE_URL
ENV GOOGLE_APPLICATION_CREDENTIALS /tmp/auth.json

RUN apk add curl tar
RUN mkdir -p /opt
RUN curl -sL ${OPTIMUS_RELEASE_URL} | tar xvz optimus
RUN mv optimus /opt/optimus || true
RUN chmod +x /opt/optimus

COPY ./entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["python3", "/opt/bumblebee/main.py"]