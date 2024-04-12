class ModelBackend:
    def process_chunk(self, strings, fp):
        array = process_chunk_to_array(self, strings)
        array.tofile(fp)
