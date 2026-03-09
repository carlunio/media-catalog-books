-- Marca para recaptura OCR los registros cuyo OCR es exactamente el prompt.
UPDATE book_items
SET
  ocr_status = NULL,
  ocr_error = 'reset_after_prompt_echo',
  ocr_provider = NULL,
  ocr_model = NULL,
  updated_at = CURRENT_TIMESTAMP
WHERE id IN (
  SELECT book_id
  FROM book_ocr_data
  WHERE lower(trim(coalesce(extracted_text, ''))) = lower(trim('Transcribe absolutamente todo el texto que veas. Respeta los saltos de linea. No inventes texto ni anadas comentarios.'))
);

DELETE FROM book_ocr_data
WHERE lower(trim(coalesce(extracted_text, ''))) = lower(trim('Transcribe absolutamente todo el texto que veas. Respeta los saltos de linea. No inventes texto ni anadas comentarios.'));
