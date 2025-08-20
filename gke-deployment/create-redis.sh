gcloud redis instances create --project=rfx-eng-tm-poc-d  notification-svc-redis-1 \
--tier=basic --size=1 --region=us-central1 --redis-version=redis_7_2 \
--network=projects/rfx-eng-tm-poc-d/global/networks/saas-net-0 \
--connect-mode=DIRECT_PEERING --display-name="notif-red-1"