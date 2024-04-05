import sys
import torch
import json
from transformers import AutoTokenizer, BloomModel

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
cpu_device = torch.device("cpu")
print(f"Using device: {device}")

### Initial version copied from model readme
print("loading tokenizer")
tokenizer = AutoTokenizer.from_pretrained('izhx/udever-bloom-560m')
print("loading model")
model = BloomModel.from_pretrained('izhx/udever-bloom-560m').cuda()
print("loaded")

boq, eoq, bod, eod = '[BOQ]', '[EOQ]', '[BOD]', '[EOD]'
eoq_id, eod_id = tokenizer.convert_tokens_to_ids([eoq, eod])

if tokenizer.padding_side != 'left':
    print('!!!', tokenizer.padding_side)
    tokenizer.padding_side = 'left'


def encode(texts: list, is_query: bool = False, max_length=2048):
    bos = boq if is_query else bod
    eos_id = eoq_id if is_query else eod_id
    texts = [bos + t for t in texts]
    encoding = tokenizer(
        texts, truncation=True, max_length=max_length - 1, padding=True
    )
    for ids, mask in zip(encoding['input_ids'], encoding['attention_mask']):
        ids.append(eos_id)
        mask.append(1)
    inputs = tokenizer.pad(encoding, return_tensors='pt')
    inputs = inputs.to(device)
    with torch.inference_mode():
        outputs = model(**inputs)
        embeds = outputs.last_hidden_state[:, -1]
    return embeds

### End of copy

def process_chunk(strings, fp):
    tensor = encode(strings)
    tensor = tensor.to(cpu_device)
    array = tensor.numpy().astype('float32')
    array.tofile(fp)

if __name__ == '__main__':
    # Check if the number of arguments is correct
    if len(sys.argv) != 3:
        print("Usage: vectorize <input_file> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    chunk = []
    with open(output_file, 'w') as output_fp:
        with open(input_file, 'r') as input_fp:
            for line in input_fp:
                json_str = json.loads(line)
                chunk.append(json_str)
                if len(chunk) == 100:
                  process_chunk(chunk, output_fp)
                  chunk = []
        if len(chunk) != 0:
              process_chunk(chunk, output_fp)
