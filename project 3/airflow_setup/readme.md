# Airflow Installation in Ubuntu 22.04

## Prerequisites
1. **Podman**: Pastikan Podman sudah terinstal di sistem Anda. Jika belum, Anda bisa menginstalnya dengan perintah berikut:
    ```sh
    sudo apt update
    sudo apt install podman
    ```

2. **Podman Compose**: Pastikan Podman Compose sudah terinstal. Anda bisa menginstalnya dengan perintah berikut:
    ```sh
    sudo pip3 install podman-compose
    ```

## Pull Images
Tarik image Apache Airflow dan PostgreSQL dari registry container.
```sh
podman pull apache/airflow:2.10.0
podman pull postgres:13
```

## Docker Build & Run
1. **Build Image**: Membangun image baru dengan tag `my-airflow` dari Dockerfile yang ada di direktori saat ini.
    ```sh
    podman build -t my-airflow .
    ```

2. **Run Containers**: Menjalankan container secara terpisah (detached mode) menggunakan Podman Compose sesuai dengan konfigurasi yang ada di file `podman-compose.yml`.
    ```sh
    podman-compose up -d
    ```