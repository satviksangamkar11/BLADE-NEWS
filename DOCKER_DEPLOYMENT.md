# 🚀 Docker Deployment Guide for Telegram News Scraper

## 📋 Overview

This project has been successfully containerized using Docker for easy deployment and scalability. The application includes a FastAPI backend, modern web interface, and integration with MongoDB Atlas, Telegram APIs, and Google Gemini AI.

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Web Frontend  │    │   FastAPI App    │    │   MongoDB Atlas │
│  (HTML/CSS/JS)  │◄──►│   (Python)       │◄──►│   (External)    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │  Telegram APIs   │
                       │  Gemini AI APIs  │
                       └──────────────────┘
```

## 🚀 Quick Start

### 1. Environment Setup

1. **Create `.env` file** (copy from `.env.example` if available):
   ```bash
   cp .env.example .env
   ```

2. **Configure your credentials** in the `.env` file:
   ```env
   # Required Telegram API credentials
   TELEGRAM_API_ID=your_api_id
   TELEGRAM_API_HASH=your_api_hash
   BOT_TOKEN=your_bot_token
   CHANNEL_USERNAME=@your_channel

   # Required external services
   MONGODB_URI=mongodb+srv://user:pass@cluster.mongodb.net/
   GEMINI_API_KEY=your_gemini_key

   # Channel usernames to scrape (JSON array)
   CHANNEL_USERNAMES=["@channel1", "@channel2"]
   ```

### 2. Local Development

Start the application locally:
```bash
# Using Docker Compose (recommended)
docker-compose up

# Or build and run manually
docker build -t telegram-news-scraper:latest .
docker run -p 8000:8000 --env-file .env telegram-news-scraper:latest
```

Access the application at: http://localhost:8000

### 3. Production Deployment

For production deployment:
```bash
# Using production compose file
docker-compose -f docker-compose.prod.yml up -d

# Or build for production
docker build -t telegram-news-scraper:prod .
```

## 📁 File Structure

```
Final/
├── Dockerfile                 # Main container configuration
├── docker-compose.yml         # Local development setup
├── docker-compose.prod.yml    # Production deployment
├── .dockerignore             # Files to exclude from build context
├── main.py                   # FastAPI application
├── blade_news_frontend.html  # Web interface
├── requirements.txt          # Python dependencies
└── telegram_session.txt      # Telegram session data
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Required | Example |
|----------|-------------|----------|---------|
| `TELEGRAM_API_ID` | Telegram API ID | ✅ | `12345678` |
| `TELEGRAM_API_HASH` | Telegram API Hash | ✅ | `abcdef1234567890` |
| `BOT_TOKEN` | Telegram Bot Token | ✅ | `123456:ABC-DEF...` |
| `CHANNEL_USERNAME` | Your channel for posting | ✅ | `@YourChannel` |
| `MONGODB_URI` | MongoDB Atlas connection | ✅ | `mongodb+srv://...` |
| `GEMINI_API_KEY` | Google Gemini API key | ✅ | `AIzaSy...` |
| `CHANNEL_USERNAMES` | Channels to scrape | ✅ | `["@ch1", "@ch2"]` |

### Optional Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `BATCH_SIZE` | `8` | Messages processed per batch |
| `CONCURRENT_API_CALLS` | `4` | Simultaneous Gemini API calls |
| `INITIAL_SCRAPE_HOURS` | `10` | Initial scrape lookback period |
| `REAL_TIME_INTERVAL_MINUTES` | `30` | Scraping frequency |

## 🚀 Deployment Options

### Option 1: Docker Compose (Recommended for Development)

```bash
# Start all services
docker-compose up

# Start in background
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

### Option 2: Manual Docker Commands

```bash
# Build image
docker build -t telegram-news-scraper:latest .

# Run container
docker run -d \
  --name telegram-scraper \
  -p 8000:8000 \
  --env-file .env \
  telegram-news-scraper:latest
```

### Option 3: Production with Enhanced Security

```bash
# Build for production
docker build -t telegram-news-scraper:prod .

# Run with production settings
docker run -d \
  --name telegram-scraper-prod \
  --restart always \
  --memory 2g \
  --cpus 2 \
  --env-file .env \
  telegram-news-scraper:prod
```

## 🔍 Monitoring & Health Checks

### Health Check Endpoint

The application includes a health check endpoint at `/api/health`:

```bash
curl http://localhost:8000/api/health
```

Response:
```json
{
  "status": "healthy",
  "mongodb": "connected",
  "scheduler": "running",
  "timestamp": "2025-10-08T21:26:46.903Z"
}
```

### Container Monitoring

```bash
# Check container status
docker ps

# View logs
docker logs telegram-scraper

# Monitor resource usage
docker stats telegram-scraper
```

## 🔒 Security Considerations

### Production Security

1. **Use strong passwords** for all API keys and database credentials
2. **Enable MongoDB Atlas** network access restrictions
3. **Use Docker secrets** for sensitive environment variables
4. **Monitor API rate limits** and implement proper error handling
5. **Regularly update** the Docker image and dependencies

### Container Security

- Non-root user execution
- Read-only filesystem where possible
- Resource limits to prevent resource exhaustion
- Health checks for automatic recovery

## 🚨 Troubleshooting

### Common Issues

#### 1. Container Won't Start

```bash
# Check logs for errors
docker logs <container-name>

# Verify environment variables are loaded
docker exec <container-name> env | grep MONGODB_URI
```

#### 2. MongoDB Connection Issues

- Verify MongoDB Atlas connection string
- Check network access in MongoDB Atlas dashboard
- Ensure database user has proper permissions

#### 3. Telegram API Issues

- Verify API credentials from https://my.telegram.org
- Check bot token from @BotFather
- Ensure bot has admin access to the channel

#### 4. Gemini AI Issues

- Verify API key from Google AI Studio
- Check API quota and rate limits
- Monitor usage in Google Cloud Console

### Debug Mode

Enable debug logging by setting:
```env
LOG_LEVEL=DEBUG
```

### Resource Issues

If experiencing memory or CPU issues:

```bash
# Check resource usage
docker stats

# Increase limits in docker-compose.prod.yml
deploy:
  resources:
    limits:
      memory: 4G  # Increase if needed
      cpus: '2.0'  # Increase if needed
```

## 📊 Performance Optimization

### Scaling Considerations

1. **Horizontal Scaling**: Run multiple scraper instances
2. **Load Balancing**: Use reverse proxy for web requests
3. **Database Optimization**: Monitor MongoDB Atlas performance
4. **Caching Strategy**: Implement Redis for session caching

### Resource Tuning

```yaml
# Example resource configuration
deploy:
  resources:
    limits:
      cpus: '2.0'
      memory: 2G
    reservations:
      cpus: '1.0'
      memory: 1G
```

## 🔄 CI/CD Integration

### Example GitHub Actions Workflow

```yaml
name: Deploy to Production
on:
  push:
    branches: [ main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    - name: Build and push Docker image
      run: |
        docker build -t telegram-scraper:${{ github.sha }} .
        docker tag telegram-scraper:${{ github.sha }} myregistry/telegram-scraper:latest
        docker push myregistry/telegram-scraper:latest
```

## 📞 Support

For issues or questions:

1. Check the troubleshooting section above
2. Review container logs: `docker logs <container-name>`
3. Verify environment variables are properly configured
4. Test external service connectivity (MongoDB, Telegram, Gemini AI)

---

**🎉 Your Telegram News Scraper is now successfully containerized and ready for deployment!**

Access your application at: http://localhost:8000

