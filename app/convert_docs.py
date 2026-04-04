from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import os

docs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Test_documents")
for txt_file in os.listdir(docs_dir):
    if txt_file.endswith(".txt"):
        pdf_path = os.path.join(docs_dir, txt_file.replace(".txt", ".pdf"))
        c = canvas.Canvas(pdf_path, pagesize=letter)
        with open(os.path.join(docs_dir, txt_file), "r", encoding="utf-8") as f:
            y = 750
            for line in f.readlines():
                c.drawString(50, y, line.strip())
                y -= 15
                if y < 50:
                    c.showPage()
                    y = 750
        c.save()
        print(f"OK: {pdf_path}")
print("DONE - all converted")
