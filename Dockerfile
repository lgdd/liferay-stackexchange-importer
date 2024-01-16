FROM python:3.10

ARG USERNAME=ray
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

RUN mkdir /app && \
    chown $USER_GID:$USER_GID /app

USER $USERNAME

WORKDIR /app

COPY topics.txt .
COPY requirements.txt .
COPY main.py .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]