# 🚀 HIZLI BAŞLANGIÇ - GitHub'a Yükleme

## 📋 YOL HARİTASI

```
┌─────────────────────────────────────────────────────────────────┐
│  1. Repo Yapısını Oluştur                                       │
│  2. Dosyaları Kopyala                                           │
│  3. Git Initialize                                              │
│  4. GitHub'a Push                                               │
│  5. Duyur ve Paylaş                                             │
└─────────────────────────────────────────────────────────────────┘
```

---

## METOD 1: Otomatik Kurulum (Önerilen) 🤖

### Adım 1: Yeni dizinde repository oluştur

```bash
# Terminal'i aç ve istediğin dizine git
cd ~/github  # veya istediğin bir yer

# Repo yapısını otomatik oluştur
bash /mnt/user-data/outputs/aml-transaction-monitoring/scripts/create_repo_structure.sh

# Dizine gir
cd aml-transaction-monitoring
```

### Adım 2: Tüm dosyaları kopyala

```bash
# Tüm dosyaları otomatik kopyala
bash /mnt/user-data/outputs/aml-transaction-monitoring/scripts/copy_files.sh .
```

### Adım 3: Git işlemleri

```bash
# Git initialize (eğer script yapmadıysa)
git init

# Tüm dosyaları ekle
git add .

# İlk commit
git commit -m "Initial commit: Real-time AML Transaction Monitoring System

- Kafka + Spark Streaming pipeline (5M TPS)
- Delta Lake for ACID storage
- AWS Glue jobs for batch processing
- Airflow orchestration
- Redshift star schema analytics
- Comprehensive data quality framework
- Production-ready with tests and CI/CD"
```

### Adım 4: GitHub'a push

```bash
# GitHub'da repo oluştur: https://github.com/new
# Repository name: aml-transaction-monitoring
# Public repository
# NO README, NO .gitignore (we have them)

# Remote ekle (YOUR_USERNAME değiştir)
git remote add origin https://github.com/YOUR_USERNAME/aml-transaction-monitoring.git

# Push
git branch -M main
git push -u origin main
```

---

## METOD 2: Manuel Kurulum 📝

### Adım 1: Dizin yapısını oluştur

```bash
cd ~/github
mkdir -p aml-transaction-monitoring && cd aml-transaction-monitoring

# Dizinleri oluştur
mkdir -p .github/workflows
mkdir -p docs
mkdir -p src/{common,glue_jobs,data_generator}
mkdir -p airflow/dags
mkdir -p sql/ddl
mkdir -p tests/unit
mkdir -p infrastructure/docker
mkdir -p scripts
mkdir -p data/sample

# Python packages
touch src/__init__.py
touch src/common/__init__.py
touch src/glue_jobs/__init__.py
touch src/data_generator/__init__.py
touch tests/__init__.py
touch tests/unit/__init__.py
```

### Adım 2: Dosyaları manuel kopyala

**Dosya Kopyalama Tablosu:**

| Kaynak Dosya | Hedef Konum |
|--------------|-------------|
| README.md | `.` (root) |
| PROJECT_SUMMARY.md | `.` (root) |
| .gitignore | `.` (root) |
| requirements.txt | `.` (root) |
| setup.py | `.` (root) |
| .github/workflows/ci.yml | `.github/workflows/` |
| docs/*.md (4 dosya) | `docs/` |
| src/common/*.py (4 dosya) | `src/common/` |
| src/glue_jobs/*.py (4 dosya) | `src/glue_jobs/` |
| src/data_generator/*.py (1 dosya) | `src/data_generator/` |
| airflow/dags/*.py (2 dosya) | `airflow/dags/` |
| sql/ddl/*.sql (2 dosya) | `sql/ddl/` |
| tests/unit/*.py (1 dosya) | `tests/unit/` |
| infrastructure/docker/docker-compose.yml | `infrastructure/docker/` |
| scripts/*.sh (3 dosya) | `scripts/` |
| data/sample/customer_profiles.csv | `data/sample/` |

**Kopyalama Komutu (her dosya için):**

```bash
# Kaynak dizinden kopyala
cp /mnt/user-data/outputs/aml-transaction-monitoring/DOSYA_ADI HEDEF_KONUM/
```

### Adım 3: Script'leri executable yap

```bash
chmod +x scripts/*.sh
```

### Adım 4: Git ve GitHub (yukarıdaki gibi)

---

## 📊 DOSYA YERLEŞİM TABLOSU

### Root Level (7 dosya)
```
├── README.md
├── PROJECT_SUMMARY.md
├── GITHUB_UPLOAD_GUIDE.md
├── FILE_PLACEMENT_GUIDE.md
├── .gitignore
├── requirements.txt
└── setup.py
```

### Proje Dosyaları (27 dosya)
```
├── .github/workflows/ci.yml
├── docs/ (4 dosya)
│   ├── ARCHITECTURE.md
│   ├── DATA_MODEL.md
│   ├── INTERVIEW_GUIDE.md
│   └── SETUP.md
│
├── src/
│   ├── common/ (5 dosya)
│   ├── glue_jobs/ (5 dosya)
│   └── data_generator/ (2 dosya)
│
├── airflow/dags/ (2 dosya)
├── sql/ddl/ (2 dosya)
├── tests/unit/ (2 dosya)
├── infrastructure/docker/ (1 dosya)
├── scripts/ (3 dosya)
└── data/sample/ (1 dosya)
```

---

## ✅ KONTROL LİSTESİ

### Yapı Kontrolü
```bash
# Dizin sayısı (20 olmalı)
find . -type d | wc -l

# Python dosyası sayısı (15+ olmalı)
find . -name "*.py" | wc -l

# Markdown dosyası sayısı (8+ olmalı)
find . -name "*.md" | wc -l

# Toplam dosya sayısı (35+ olmalı)
find . -type f | wc -l
```

### Git Kontrolü
```bash
# Git durumu
git status

# Commit geçmişi
git log --oneline

# Remote kontrol
git remote -v
```

---

## 🎯 GitHub Repository Ayarları

### Repository Oluştururken:
- ✅ **Name**: `aml-transaction-monitoring`
- ✅ **Description**: `Real-time AML transaction monitoring with Kafka, Spark Streaming, Delta Lake, and AWS Glue`
- ✅ **Visibility**: Public
- ❌ **NO** README initialization
- ❌ **NO** .gitignore initialization
- ❌ **NO** License selection (we'll add later)

### Repository Oluşturduktan Sonra:

#### 1. Topics Ekle (Settings → General)
```
data-engineering, spark-streaming, delta-lake, kafka, 
aws-glue, airflow, real-time-processing, aml, 
compliance, python, pyspark
```

#### 2. About Bölümünü Düzenle
- Website: (varsa GitHub Pages URL'i)
- Topics: (yukarıdakiler)
- ✅ Releases
- ✅ Packages
- ✅ Deployments

#### 3. README Badges Ekle (README.md başına)
```markdown
![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange)
![AWS](https://img.shields.io/badge/AWS-Glue%20%7C%20S3%20%7C%20Redshift-yellow?logo=amazon-aws)
![Kafka](https://img.shields.io/badge/Kafka-3.5-black?logo=apache-kafka)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-blue)
![License](https://img.shields.io/badge/License-MIT-green)
```

---

## 🔍 SORUN GİDERME

### Dosya Bulunamıyor
```bash
# Kaynak dizini kontrol et
ls -la /mnt/user-data/outputs/aml-transaction-monitoring/

# Eğer dosyalar sidebar'da görünüyorsa, oradan indir
```

### Git Push Hatası
```bash
# Remote kontrol et
git remote -v

# Doğru remote ekle
git remote set-url origin https://github.com/YOUR_USERNAME/aml-transaction-monitoring.git

# Tekrar push
git push -u origin main
```

### Permission Denied (scripts)
```bash
# Tüm scriptleri executable yap
chmod +x scripts/*.sh
```

---

## 📱 LinkedIn Paylaşım Şablonu

```
🚀 Yeni portfolio projemi tamamladım!

Real-Time AML Transaction Monitoring System

Özellikler:
✅ Kafka + Spark Streaming (5M işlem/gün)
✅ Delta Lake (ACID garantisi)
✅ AWS Glue batch jobs
✅ Airflow orkestrasyon
✅ Redshift star schema
✅ Kapsamlı data quality framework

Teknik Stack: 
PySpark, AWS (Glue, S3, Redshift), Kafka, Delta Lake, 
Airflow, Docker, Python

Bu proje, batch processing (Basel RWA - 120M kayıt/gün) 
deneyimime ek olarak real-time streaming uzmanlığımı 
gösteriyor.

GitHub: [LINK]

#DataEngineering #AWS #Spark #Kafka #DeltaLake 
#Berlin #JobSearch #Portfolio
```

---

## 🎉 BAŞARIYLA TAMAMLANDI!

Projeniz artık GitHub'da ve Berlin'deki işverenler için hazır! 🇩🇪

**Son Kontrol:**
- [ ] GitHub'da repository oluşturuldu
- [ ] Tüm dosyalar push edildi
- [ ] README düzgün görünüyor
- [ ] Topics eklendi
- [ ] LinkedIn'de paylaşıldı
- [ ] CV'de belirtildi

---

**İyi şanslar! 🚀**
