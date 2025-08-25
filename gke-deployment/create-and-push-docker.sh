image_name="us-central1-docker.pkg.dev/rfx-eng-tm-poc-d/poc-images/reactive-sse-app:v2.1.5"

docker rmi $image_name 2>/dev/null || echo "Image not found, continuing..."

docker build -t $image_name ../. || {
    echo "Failed to build Docker image"
    exit 1
}

docker push $image_name || {
    echo "Failed to push Docker image"
    exit 1
}