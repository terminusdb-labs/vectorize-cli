from sentence_transformers import SentenceTransformer

class MxbaiBackend:
    def __init__(self):
        self.model = SentenceTransformer("mixedbread-ai/mxbai-embed-large-v1")

    def process_chunk(self, strings, fp):
        array = self.model.encode(strings)
        array.tofile(fp)
