FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy

WORKDIR /app

# 1) Install deps first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2) Ensure Chromium is available
RUN python -m playwright install --with-deps chromium

# 3) Add the code
COPY . .

# 4) Install project package (scripts rely on editable import)
RUN pip install --no-cache-dir -e .

# 5) Sanity check
RUN python -m playwright --version && python -c "import sys; import playwright; print('PLAYWRIGHT_OK', sys.version)"

# (No ENTRYPOINT here; Cloud Run Jobs will pass the command/args)
