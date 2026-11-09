# Deploying LinkPulse for free

| Piece | Service | Free tier notes |
|---|---|---|
| API, dashboard, analytics consumer | [Render](https://render.com) web service | Sleeps after ~15 min idle; first request then takes ~1 min |
| PostgreSQL | [Neon](https://neon.tech) | Scales to zero; wakes in about a second |
| Redis | [Upstash](https://upstash.com) | Request quota per month; plenty for a demo |
| Kafka | [Aiven](https://aiven.io/free-kafka) | Small throughput limits; powers off when idle |

None of these need a credit card. The whole setup takes about 20 minutes. Do the steps in order and keep a text file open to collect the values.

> **Never commit these values to git.** They go only into Render's environment settings.

## 1. Postgres on Neon → `DATABASE_URL`

1. Sign up at neon.tech and create a project named `linkpulse`. Pick the region closest to Singapore (or wherever you'll put Render).
2. On the project dashboard, click **Connect**.
3. **Turn off "Connection pooling"**, so the host name has no `-pooler`. Copy the connection string. It looks like:
   `postgresql://neondb_owner:...@ep-xxx.ap-southeast-1.aws.neon.tech/neondb?sslmode=require`

Tables are created automatically when the app starts.

## 2. Redis on Upstash → `REDIS_URL`

1. Sign up at upstash.com and create a **Redis** database named `linkpulse`, in a region near Render's.
2. On the database page, find the connection details for **TCP** / `redis-cli`, with TLS enabled. Copy the URL that starts with **`rediss://`** (two s's means TLS):
   `rediss://default:...@xxx.upstash.io:6379`

## 3. Kafka on Aiven → `KAFKA_BROKERS`, `KAFKA_CA_CERT`, `KAFKA_CLIENT_CERT`, `KAFKA_CLIENT_KEY`

1. Sign up at aiven.io and create a service: **Apache Kafka**, **Free plan**. Wait until it shows **Running** (a few minutes).
2. Open **Topics** → **Create topic**. Name it `link-clicks` (exactly), with **2 partitions**. Auto-creation isn't available on the free plan, so this step is required.
3. Open the service **Overview** → **Connection information**, with authentication method **Client certificate**:
   - **Service URI** (`xxx.aivencloud.com:12345`) → this is `KAFKA_BROKERS`
   - Download **CA certificate** (`ca.pem`) → its whole content is `KAFKA_CA_CERT`
   - Download **Access certificate** (`service.cert`) → `KAFKA_CLIENT_CERT`
   - Download **Access key** (`service.key`) → `KAFKA_CLIENT_KEY`

Paste each file's full text, including the `-----BEGIN ...-----` and `-----END ...-----` lines. Render accepts multi-line values.

If you'd rather use a username and password: enable SASL in the service's advanced configuration, then use the SASL port as `KAFKA_BROKERS`, set `KAFKA_SASL_MECHANISM=SCRAM-SHA-256`, `KAFKA_USERNAME` and `KAFKA_PASSWORD`, keep `KAFKA_CA_CERT`, and leave the client cert and key empty.

## 4. The app on Render

1. Sign up at render.com with your GitHub account, and allow access to the `LinkPulse` repository.
2. Click **New** → **Blueprint**, pick `Harisankar2000/LinkPulse`. Render reads [`render.yaml`](render.yaml).
3. It asks for the six secret values. Paste them from the steps above, then click **Apply**.
4. The first build takes 3–5 minutes. When it's live, open `https://<your-service>.onrender.com`.

`BASE_URL` is set automatically from Render's public URL (`RENDER_EXTERNAL_URL`), so short links point at the right host.

## 5. Check it works

- `https://<your-service>.onrender.com/readyz` should show `{"postgres":"ok","redis":"ok"}`.
- On the dashboard, shorten a link, click **Simulate 50 clicks**, and the charts should fill within about 10 seconds.
- If the charts stay empty, check Render's **Logs** for `kafka` errors. Usually it's a missing `link-clicks` topic, a wrong broker port, or a certificate pasted without its BEGIN/END lines.

## Before an interview

The free Render service sleeps when idle, and Aiven's free Kafka powers off. **Open the site about 5 minutes before** and click through once, so everything is warm. If Kafka was powered off, start it again from the Aiven console.

## How the hosted version differs from `docker compose`

| | Local (`make up`) | Render free |
|---|---|---|
| Analytics consumer | Separate container | Same process (`ANALYTICS_IN_PROCESS=true`), since background workers aren't free |
| Kafka | Plaintext, 6 partitions | TLS with client certificates, 2 partitions |
| Country data | Shows `XX` unless you send a header | Shows `XX` unless the request passes through a CDN that adds a country header; the click simulator generates realistic countries |

The code path is identical in both: redirects still publish to Kafka, and the consumer still batches into Postgres rollups.
