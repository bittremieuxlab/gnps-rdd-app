# app/Dockerfile

FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*


# Install the exact versions listed in requirements.lock, so every build of
# this commit contains the same packages. See the header of that file for how
# to regenerate it; REQUIREMENTS=requirements.txt builds unpinned for that.
ARG REQUIREMENTS=requirements.lock
COPY ./requirements.txt ./requirements.lock /app/

RUN pip3 install -r ${REQUIREMENTS}

COPY . /app

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "Home.py", "--server.port=8501", "--server.address=0.0.0.0"]