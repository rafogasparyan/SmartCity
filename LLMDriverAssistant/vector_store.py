from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings
from pathlib import Path
from settings import VECTOR_DIR

def get_vectordb():
    if Path(VECTOR_DIR).exists():
        return Chroma(persist_directory=VECTOR_DIR,
                      embedding_function=OpenAIEmbeddings())
    
    docs = [
        "If road is wet, increase braking distance by 2x normal dry distance.",
        "If visibility < 100 meters, maximum speed should be 50 km/h or less.",
        "During heavy rain, slow down by 20% below posted speed limits.",
        "If there is ice warning, reduce speed to below 40 km/h and avoid sudden steering.",
        "Turn headlights ON during fog, heavy rain, or snow.",
        "In case of emergency vehicle nearby, slow down and move to the right lane.",
        "Maintain at least 4-second gap between vehicles during snow.",
        "Reduce speed when approaching intersections with limited visibility."
    ]

    vectordb = Chroma.from_texts(
        [d[0] for d in docs],
        embedding=OpenAIEmbeddings(),
        persist_directory=VECTOR_DIR,
    )
    
    return vectordb
