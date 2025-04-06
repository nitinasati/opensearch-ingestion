@echo off
echo Running OpenSearch Ingestion Tests...

REM Install coverage if not already installed
pip install coverage pytest pytest-cov

REM Run tests with coverage
echo.
echo Running tests with coverage...
python -m pytest tests/ --cov=. --cov-report=xml --cov-report=term-missing --junitxml=test-results.xml

echo.
echo All tests completed.

pause 