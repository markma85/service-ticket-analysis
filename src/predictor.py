def summarize_ticket(model, tokenizer, ticket_text):
    """
    Generate a summary for a single ticket using the trained model.
    """
    inputs = tokenizer(ticket_text, return_tensors="pt", truncation=True, max_length=512)
    outputs = model.generate(inputs.input_ids, max_length=128)
    return tokenizer.decode(outputs[0], skip_special_tokens=True)

def batch_summarize_tickets(model, tokenizer, tickets):
    """
    Generate summaries for a batch of tickets.
    """
    return [summarize_ticket(model, tokenizer, ticket) for ticket in tickets]