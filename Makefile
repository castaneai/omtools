build-image-cleaner:
	 KO_DOCKER_REPO=ghcr.io/castaneai/omtools/om-ticket-cleaner ko build --bare cmd/om-ticket-cleaner/main.go