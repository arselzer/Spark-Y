#!/bin/bash
set -e

echo "Starting Yannasparkis backend..."
echo "PySpark variant: ${PYSPARK_VARIANT:-custom-local}"

# If using custom-local variant, check for and install local PySpark package
if [ "$PYSPARK_VARIANT" = "custom-local" ]; then
    echo "Checking for local PySpark package..."

    # Look for PySpark package files in /app/pyspark directory
    if ls /app/pyspark/*.tar.gz 1> /dev/null 2>&1; then
        PYSPARK_PACKAGE=$(ls /app/pyspark/*.tar.gz | head -n 1)
        echo "Found local PySpark package: $PYSPARK_PACKAGE"
        echo "Installing custom PySpark from local file..."
        pip install --no-cache-dir "$PYSPARK_PACKAGE"
        echo "Custom PySpark installed successfully!"
    elif ls /app/pyspark/*.whl 1> /dev/null 2>&1; then
        PYSPARK_PACKAGE=$(ls /app/pyspark/*.whl | head -n 1)
        echo "Found local PySpark wheel: $PYSPARK_PACKAGE"
        echo "Installing custom PySpark from local file..."
        pip install --no-cache-dir "$PYSPARK_PACKAGE"
        echo "Custom PySpark installed successfully!"
    else
        echo "WARNING: PYSPARK_VARIANT=custom-local but no PySpark package found in /app/pyspark/"
        echo "Expected: .tar.gz or .whl file"
        echo "Falling back to standard PySpark installation..."
        pip install --no-cache-dir pyspark==3.5.0 py4j==0.10.9.7
    fi
fi

# Display PySpark version
echo "PySpark version:"
python3 -c "import pyspark; print(pyspark.__version__)" || echo "PySpark not found!"

# Execute the main command
echo "Starting application with command: $@"
exec "$@"
