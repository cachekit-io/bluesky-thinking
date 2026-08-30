# LAB-1131 scratch: genuine violation — unpinned base image, no @sha256 digest.
# The base-image pinning rule MUST still fire on this file (high severity).
FROM python:3.12

WORKDIR /app
COPY . .
CMD ["python", "-c", "print('lab-1131 scratch')"]
