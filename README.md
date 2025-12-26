# Media Organizer by Real Date

Script em Python para **organizar fotos e vídeos pela data real de criação**, com **deduplicação rápida**, **tolerância a arquivos corrompidos** e **execuções incrementais seguras**.  
Projetado para acervos grandes, backups antigos, NAS e múltiplas execuções.

---

## ✨ Principais Funcionalidades

- 📸 **Fotos e vídeos**
- 🗓️ Organização por **data real** (EXIF / QuickTime)
- ⚡ **Deduplicação rápida** via hash parcial (CRC32 – 64 KB)
- 🔁 **Execuções incrementais**
- 🧠 **Inventário persistente**
- 🛡️ **Origem nunca é modificada**
- 💥 **Tolerante a erros** (arquivos corrompidos, metadata inválida, timestamps absurdos)
- 📊 **Estatísticas finais detalhadas**
- 📈 **Progresso a cada 1%**
- 🗂️ Organização automática em `YYYY/MM`

---

## 📂 Estrutura de Saída

```text
DESTINO/
 ├── 2012/
 │   └── 07/
 │       ├── foto.jpg
 │       └── video.mp4
 ├── 2019/
 │   └── 11/
 └── _duplicates/
     └── 2015/
         └── 03/

