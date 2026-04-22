# GNPS RDD App

A Streamlit web application for Reference Data-Driven (RDD) analysis of GNPS molecular networking data.

## Overview

This application helps researchers analyze GNPS (Global Natural Products Social Molecular Networking) data using a reference-driven approach. It enables ontology-based counting, visualization, and statistical analysis of metabolomics data.

## Features

- **RDD Count Table Generation** - Create reference data-driven counts from GNPS networks
- **Interactive Visualizations** - Box plots, heatmaps, and bar charts for data exploration
- **PCA Analysis** - Principal Component Analysis with CLR transformation
- **Sankey Diagrams** - Flow visualization of metabolite classifications
- **GNPS Integration** - Direct access via Task ID or file upload

## 🌐 Live App

Access the app online at: **https://gnpsrdd.streamlit.app/**

## Installation

### Requirements

- Python 3.11+
- pip

## Deployment

To deploy the app using Docker:

```bash
# Build and start services in detached mode
./deploy.sh
```

This script builds the Docker images and starts the services defined in `docker-compose.deployment.yaml`.

## Local Development

To run the app locally:

```bash
# Clone the repository
git clone https://github.com/AlejandroMC28/gnps_rdd_app.git
cd gnps_rdd_app

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the app
streamlit run Home.py
```

The app will open in your default browser at `http://localhost:8501`

## Usage

### 1. Create RDD Count Table
- Upload GNPS network file or enter GNPS Task ID
- Select sample types (simple/complex/all)
- Choose sample groups
- Generate RDD counts across ontology levels

### 2. Visualize Data
- Create box plots, heatmaps, and bar charts
- Compare different sample groups
- Explore reference type distributions

### 3. PCA Analysis
- Perform dimensionality reduction
- Apply CLR transformation
- Visualize sample clustering

### 4. Sankey Diagrams
- Visualize metabolite classification flows
- Track ontology hierarchies


## Testing

```bash
# Run all tests
make test

# Or using pytest directly
pytest -v

# Check code quality
make lint

# Format code
make format
```


## Development

### Key Dependencies

- **Streamlit** - Web application framework
- **Pandas/NumPy** - Data manipulation
- **Plotly/Matplotlib** - Visualizations
- **scikit-learn** - PCA analysis
- **scikit-bio** - CLR transformation
- **gnpsdata** - GNPS data access

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `make test`
5. Format code: `make format`
6. Submit a pull request

## License

Apache license 2.0

## Citation

If you use this tool in your research, please cite:

```
[Citation information to be added]
```

## Contact

For questions or issues, please open a GitHub issue.

