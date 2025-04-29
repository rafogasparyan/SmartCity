from langchain_community.document_loaders import PyPDFLoader, TextLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings
from pathlib import Path
from settings import VECTOR_DIR

def get_vectordb():
    if Path(VECTOR_DIR).exists():
        print("Using existing vector DB")
        return Chroma(persist_directory=VECTOR_DIR, embedding_function=OpenAIEmbeddings())

    print("📚 Loading documents from /app/resources/")
    loaders = []

    for file in Path("/app/resources").glob("*"):
        if file.suffix.lower() == ".pdf":
            loaders.append(PyPDFLoader(str(file)))
        elif file.suffix.lower() == ".txt":
            loaders.append(TextLoader(str(file)))

    documents = []
    for loader in loaders:
        documents.extend(loader.load())

    splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=100)
    chunks = splitter.split_documents(documents)

    vectordb = Chroma.from_documents(
        chunks,
        embedding=OpenAIEmbeddings(),
        persist_directory=VECTOR_DIR
    )

#     vectordb.persist()
    print(" Vector DB created and persisted")
    return vectordb
