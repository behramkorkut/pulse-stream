# Un seul Dockerfile pour les quatre programmes : on choisit lequel compiler avec --build-arg CMD=...
#   docker build --build-arg CMD=collector --build-arg VERSION=$(git describe --tags --always) -t pulse-stream/collector .
# (make docker-build construit les quatre.)

ARG GO_VERSION=1.27.1

# ---------- Étape 1 : compilation (grosse image, jetée après usage) ----------
FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src

# Les dépendances d'abord : cette couche n'est reconstruite que si go.mod ou go.sum changent,
# pas à chaque modification du code. (Volontairement sans `RUN --mount=type=cache` : ce Dockerfile reste
# compatible avec l'ancien constructeur de Docker comme avec BuildKit.)
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

ARG CMD
ARG VERSION=dev
# Renseignés par BuildKit (vides avec l'ancien constructeur : Go prend alors la plateforme de la machine).
ARG TARGETOS
ARG TARGETARCH

# CGO_ENABLED=0 : binaire 100 % statique, sans dépendance à une bibliothèque C du système.
# -trimpath retire les chemins de la machine de build ; -s -w retirent les symboles de débogage (binaire plus petit).
RUN test -n "${CMD}" || { echo "ERREUR : --build-arg CMD=<collector|processor|aggregator|loadgen> est obligatoire" >&2; exit 1; } && \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -trimpath \
      -ldflags "-s -w -X github.com/behramkorkut/pulse-stream/internal/version.Version=${VERSION}" \
      -o /out/app "./cmd/${CMD}"

# ---------- Étape 2 : image finale (minuscule, sans shell ni gestionnaire de paquets) ----------
# distroless/static : certificats TLS, fuseaux horaires et un utilisateur sans privilèges, rien d'autre.
# Pas de shell dans l'image : un attaquant qui prendrait la main n'a presque aucun outil sous la main.
FROM gcr.io/distroless/static-debian12:nonroot

ARG CMD
ARG VERSION=dev
LABEL org.opencontainers.image.title="pulse-stream ${CMD}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.source="https://github.com/behramkorkut/pulse-stream"

COPY --from=build /out/app /app

# 65532 = utilisateur « nonroot » de l'image distroless. Écrit explicitement : Kubernetes (runAsNonRoot)
# exige un identifiant numérique pour vérifier qu'on ne tourne pas en root.
USER 65532:65532
ENTRYPOINT ["/app"]
