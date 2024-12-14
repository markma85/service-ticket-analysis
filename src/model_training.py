from datasets import Dataset
from transformers import T5Tokenizer, T5ForConditionalGeneration, Trainer, TrainingArguments

def train_model(data, model_name="t5-small", output_dir="./results"):
    """
    Fine-tune the T5 model for generating question summaries.
    """
    # Load tokenizer and pre-trained model
    tokenizer = T5Tokenizer.from_pretrained(model_name)
    model = T5ForConditionalGeneration.from_pretrained(model_name)

    # Prepare dataset
    dataset = Dataset.from_pandas(data)
    dataset = dataset.train_test_split(test_size=0.1)
    train_dataset = dataset['train']
    eval_dataset = dataset['test']

    # Data preprocessing
    def preprocess_function(examples):
        inputs = tokenizer(examples['input'], max_length=512, truncation=True)
        labels = tokenizer(examples['BF'], max_length=128, truncation=True)
        inputs['labels'] = labels['input_ids']
        return inputs

    tokenized_datasets = dataset.map(preprocess_function, batched=True)

    # Configure training parameters
    training_args = TrainingArguments(
        output_dir=output_dir,
        evaluation_strategy="epoch",
        learning_rate=5e-5,
        per_device_train_batch_size=4,
        num_train_epochs=3,
        weight_decay=0.01,
        save_total_limit=1,
        predict_with_generate=True,
    )

    # Initialize Trainer
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=tokenized_datasets['train'],
        eval_dataset=tokenized_datasets['test'],
        tokenizer=tokenizer,
    )

    # Start training
    trainer.train()
    return model, tokenizer