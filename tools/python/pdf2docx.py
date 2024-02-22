from pdf2docx import parse

PDFentrada = r'C:\Users\paulolima\Downloads\Tamplate Doc.pdf'
DOCXsaida = r'C:\Users\paulolima\Downloads\Template Doc.docx'

parse(PDFentrada, DOCXsaida, start=0, end=None)