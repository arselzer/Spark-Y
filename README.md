# Spark-Y

Interactive demonstration of **"Avoiding Materialisation for Guarded Aggregate Queries"** (presented at VLDB 2025).

This web application demonstrates query optimisation techniques in Apache Spark SQL that avoid materialising intermediate join results for acyclic queries with aggregates.

## Features

- **Hypergraph Visualisation**: Interactive visualisation of query structure showing relations and join patterns
- **Performance Comparison**: Side-by-side comparison of original vs optimised execution
- **Query Browser**: Browse and search through benchmark queries (JOB, TPC-H)
- **Detailed Metrics**: Execution time, intermediate result sizes, memory usage, and more
- **Operator Flow Visualization**: Interactive DAG showing Spark execution plan operators with color-coded nodes and data flow

## Architecture

- **Backend**: FastAPI + PySpark
- **Frontend**: Vue.js
- **Deployment**: Docker-compose

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Git
- 8GB+ RAM recommended
- Custom Spark JAR with optimization (see Setup section)

### 1. Clone the Repository

```bash
git clone https://github.com/arselzer/Spark-Y.git
cd Spark-Y
```

### 2. Set Up Custom Spark JAR

The demo requires a custom Spark build for evaluating the optimisations from the paper (although it will work with any version of Spark SQL and can also be used to compare other optimisations). There are two options:

#### Option A: Download from release

```bash
./scripts/download_spark.sh
```

This will download the custom PySpark package from GitHub releases and place it in the `pyspark/` directory.

#### Option B: Copy into directory

Copy a PySpark packacke into `pyspark/`

### 4. Start the Application

#### Production Mode (recommended)

```bash
docker-compose up
```

This starts:
- Frontend at http://localhost:3000
- Backend API at http://localhost:8000

Run in background:
```bash
docker-compose up -d
```

### 5. Access the Application

Open your browser and navigate to:
- **Application**: http://localhost:3000 (production) or http://localhost:5173 (development)
- **API Documentation**: http://localhost:8000/docs

## Project Structure

```
yannasparkis/
├── backend/                  # FastAPI backend
│   ├── app/
│   │   ├── api/             # API route handlers
│   │   │   ├── queries.py   # Query catalog management
│   │   │   ├── execution.py # Query execution and comparison
│   │   │   └── hypergraph.py# Hypergraph extraction
│   │   ├── models/          # Pydantic data models
│   │   ├── spark/           # Spark session management
│   │   ├── hypergraph/      # Hypergraph extraction logic
│   │   └── main.py          # FastAPI application
│   ├── requirements.txt
│   └── Dockerfile
│
├── frontend/                # Vue.js frontend
│   ├── src/
│   │   ├── components/      # Vue components
│   │   │   ├── QueryEditor.vue
│   │   │   ├── HypergraphViewer.vue
│   │   │   └── ExecutionComparison.vue
│   │   ├── views/           # Page views
│   │   ├── services/        # API service layer
│   │   ├── types/           # TypeScript type definitions
│   │   └── App.vue
│   ├── package.json
│   └── Dockerfile
│
├── spark-eval-groupagg/     # Git submodule - benchmark queries
├── spark/                   # Custom Spark JAR (gitignored)
├── data/                    # Query data (gitignored)
├── scripts/                 # Setup and utility scripts
├── docker-compose.yml
└── README.md
```

## Usage Guide

### Browse Queries

1. Navigate to the **Queries** page
2. Use search or filter by category (JOB, TPC-H, etc.)
3. Click on a query to view details
4. Click "Execute" to run the query

### Execute Queries

1. Navigate to the **Execute** page
2. Enter or paste your SQL query
3. Click "Execute Query"
4. View:
   - Query hypergraph structure
   - Side-by-side execution comparison
   - Performance metrics (speedup, memory reduction)
   - Operator flow DAG showing execution plan operators
   - Query results


## Development

### Backend Development

```bash
cd backend

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run development server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Frontend Development

```bash
cd frontend

# Install dependencies
npm install

# Run development server
npm run dev

# Build for production
npm run build
```

### Running Tests

```bash
# Backend tests
cd backend
pytest

# Frontend tests
cd frontend
npm run test
```

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```bash
# Spark configuration
SPARK_MASTER=local[*]
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g

# API configuration
API_HOST=0.0.0.0
API_PORT=8000

# Frontend configuration
VITE_API_URL=http://localhost:8000/api
```

### Custom Spark Configuration

Edit `backend/app/spark/spark_manager.py` to customize Spark settings:

```python
conf.set("spark.driver.memory", "4g")
conf.set("spark.executor.memory", "4g")
conf.set("spark.sql.adaptive.enabled", "false")
```

## Deployment

### Docker Production Deployment

```bash
# Build images
docker-compose build

# Start services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```
## Related Links

- [Paper (VLDB 2025)](https://www.vldb.org/pvldb/vol18/p1398-selzer.pdf)
- [Spark Evaluation Repository](https://github.com/arselzer/spark-eval-groupagg)
- [Custom Spark Fork](https://github.com/arselzer/spark)

