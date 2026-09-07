# Standard library only, so the image is just Python plus our source.
FROM python:3.11-slim
WORKDIR /app
COPY ripple/ /app/ripple/
ENV PYTHONUNBUFFERED=1
EXPOSE 7000 8080
ENTRYPOINT ["python", "-m", "ripple"]
