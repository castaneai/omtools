# om-ticket-cleaner

## Install

```
go install github.com/castaneai/omtools/cmd/om-ticket-cleaner@latest
```

## Usage

```
om-ticket-cleaner <REDIS_ADDR> <STALE_TIME>
```

This is an example of a Kubernetes CronJob 
that periodically deletes tickets created more than 10 minutes ago.

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: om-ticket-cleaner
  namespace: open-match
spec:
  schedule: "* * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: om-ticket-cleaner
              image: ghcr.io/castaneai/omtools/om-ticket-cleaner:latest
              args:
                - "redis-host:6379"
                - "10m"
          restartPolicy: OnFailure
```
