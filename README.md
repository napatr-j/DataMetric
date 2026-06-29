# DataMetric
A web-based social media analytics platform that enables users to securely scrape and monitor their social media metrics, including followers, following, likes, and engagement. Powered by Apache Airflow for scheduled data collection, with a Data Lake and Data Warehouse architecture for historical analysis and interactive comparison dashboards.

---

## Getting Started

### Prerequisites
- Docker & Docker Compose installed

### 1. Create `.env` file

Create a `.env` file in the project root with the following variables:

```env
# Airflow
AIRFLOW_UID=50000

# YouTube target account
YOUTUBE_ACCOUNT=https://www.youtube.com/@<channel-handle>

# MinIO (object storage / data lake)
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=<your-minio-access-key>
MINIO_SECRET_KEY=<your-minio-secret-key>
MINIO_BUCKET=<your-bucket-name>

# Email notification (Gmail SMTP)
EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_USERNAME=<your-gmail-address>
EMAIL_PASSWORD=<your-gmail-app-password>
EMAIL_RECEIVER=<recipient-email-address>
```

> **Note:** `EMAIL_PASSWORD` should be a [Gmail App Password](https://support.google.com/accounts/answer/185833), not your regular Gmail password.

### 2. Start the services

```bash
docker-compose up -d
```

### 3. Access Airflow

Open [http://localhost:8080](http://localhost:8080) in your browser. Default credentials are `airflow` / `airflow`.

---

## Code Standards

### Commit Messages

Format: `<type>: <short description>`

| Type | When to use |
|------|-------------|
| `feat` | New feature or functionality |
| `fix` | Bug fix |
| `chore` | Maintenance, dependency updates, config changes |
| `setup` | Initial project or environment setup |
| `refactor` | Code restructuring without behavior change |
| `docs` | Documentation changes only |
| `test` | Adding or updating tests |

**Examples:**
```
feat: implement youtube scraper without login
fix: handle missing video metadata gracefully
chore: update requirements for airflow env vars
docs: add env setup instructions to readme
```

### Branch Naming

Format: `<type>-<short-description-in-kebab-case>`

**Examples:**
```
feature-scrape-youtube-without-login
fix-minio-upload-error
setup-airflow-docker-compose
chore-update-dependencies
```

### Pull Request Format

**PR Title:** `Setup / Feature / Fix / ... : Short description`

**PR Description template:**

```markdown
## Description



## Relates to issue

Closes issue 

## Changes

### Subtopic

* 
* 


## Testing

* 
*
*

## Screenshots

N/A

## Checklist

* [x] 
* [x]
```
