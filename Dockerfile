FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    # poly-eip712-structs installs as 'poly_eip712_structs' but py-clob-client imports 'eip712_structs'
    python -c "import site; open(site.getsitepackages()[0]+'/eip712_structs.py','w').write('from poly_eip712_structs import *\nfrom poly_eip712_structs import make_domain, EIP712Struct, String, Uint, Bytes, Address, Boolean, Array\n')"

COPY backend/ backend/
COPY frontend/ frontend/
COPY run.py .

EXPOSE 8000

CMD ["python", "run.py"]
