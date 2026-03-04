-- Marca para recaptura OCR los registros cuyo credits_text es exactamente el prompt OCR.
UPDATE books
SET
  credits_text = NULL,
  isbn_raw = NULL,
  isbn = NULL,
  ocr_status = NULL,
  ocr_error = 'reset_after_prompt_echo',
  ocr_provider = NULL,
  ocr_model = NULL,
  updated_at = CURRENT_TIMESTAMP
WHERE lower(trim(coalesce(credits_text, ''))) = lower(trim('Transcribe absolutamente todo el texto que veas. Respeta los saltos de linea. No inventes texto ni anadas comentarios.'));
