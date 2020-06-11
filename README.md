# postgis-helpers

To create the development environment:

```bash
(base) $ conda create --name postgis_helpers python=3.8
(base) $ conda activate postgis_helpers
(postgis_helpers) $ pip install -r requirements.txt
```

To update the documentation:
```bash
(postgis_helpers) $ sphinx-apidoc --separate --module-first -o ./ ../postgis_helpers
```