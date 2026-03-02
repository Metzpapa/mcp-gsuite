from googleapiclient.discovery import build 
from . import gauth
import logging
import base64
import traceback
from email.mime.text import MIMEText
from typing import Tuple, List


class GmailService():
    def __init__(self, user_id: str):
        credentials = gauth.get_stored_credentials(user_id=user_id)
        if not credentials:
            raise RuntimeError("No Oauth2 credentials stored")
        self.service = build('gmail', 'v1', credentials=credentials)

    def _parse_message(self, txt, parse_body=False) -> dict | None:
        """
        Parse a Gmail message into a structured format.
        
        Args:
            txt (dict): Raw message from Gmail API
            parse_body (bool): Whether to parse and include the message body (default: False)
        
        Returns:
            dict: Parsed message containing comprehensive metadata
            None: If parsing fails
        """
        try:
            message_id = txt.get('id')
            thread_id = txt.get('threadId')
            payload = txt.get('payload', {})
            headers = payload.get('headers', [])

            metadata = {
                'id': message_id,
                'threadId': thread_id,
                'historyId': txt.get('historyId'),
                'internalDate': txt.get('internalDate'),
                'sizeEstimate': txt.get('sizeEstimate'),
                'labelIds': txt.get('labelIds', []),
                'snippet': txt.get('snippet'),
            }

            for header in headers:
                name = header.get('name', '').lower()
                value = header.get('value', '')
                
                if name == 'subject':
                    metadata['subject'] = value
                elif name == 'from':
                    metadata['from'] = value
                elif name == 'to':
                    metadata['to'] = value
                elif name == 'date':
                    metadata['date'] = value
                elif name == 'cc':
                    metadata['cc'] = value
                elif name == 'bcc':
                    metadata['bcc'] = value
                elif name == 'message-id':
                    metadata['message_id'] = value
                elif name == 'in-reply-to':
                    metadata['in_reply_to'] = value
                elif name == 'references':
                    metadata['references'] = value
                elif name == 'delivered-to':
                    metadata['delivered_to'] = value

            if parse_body:
                body = self._extract_body(payload)
                if body:
                    metadata['body'] = body

                metadata['mimeType'] = payload.get('mimeType')

            return metadata

        except Exception as e:
            logging.error(f"Error parsing message: {str(e)}")
            logging.error(traceback.format_exc())
            return None

    def _extract_body(self, payload) -> str | None:
        """
        Extract the email body from the payload.
        Handles both multipart and single part messages, including nested multiparts.
        """
        try:
            # For single part text/plain messages
            if payload.get('mimeType') == 'text/plain':
                data = payload.get('body', {}).get('data')
                if data:
                    return base64.urlsafe_b64decode(data).decode('utf-8')
            
            # For single part text/html messages
            if payload.get('mimeType') == 'text/html':
                data = payload.get('body', {}).get('data')
                if data:
                    return base64.urlsafe_b64decode(data).decode('utf-8')
            
            # For multipart messages (both alternative and related)
            if payload.get('mimeType', '').startswith('multipart/'):
                parts = payload.get('parts', [])
                
                # First try to find a direct text/plain part
                for part in parts:
                    if part.get('mimeType') == 'text/plain':
                        data = part.get('body', {}).get('data')
                        if data:
                            return base64.urlsafe_b64decode(data).decode('utf-8')
                
                # If no direct text/plain, recursively check nested multipart structures
                for part in parts:
                    if part.get('mimeType', '').startswith('multipart/'):
                        nested_body = self._extract_body(part)
                        if nested_body:
                            return nested_body
                            
                # If still no body found, try the first part as fallback
                if parts and 'body' in parts[0] and 'data' in parts[0]['body']:
                    data = parts[0]['body']['data']
                    return base64.urlsafe_b64decode(data).decode('utf-8')

            return None

        except Exception as e:
            logging.error(f"Error extracting body: {str(e)}")
            return None

    def query_emails(self, query=None, max_results=100):
        """
        Query emails from Gmail based on a search query.
        
        Args:
            query (str, optional): Gmail search query (e.g., 'is:unread', 'from:example@gmail.com')
                                If None, returns all emails
            max_results (int): Maximum number of emails to retrieve (1-500, default: 100)
        
        Returns:
            list: List of parsed email messages, newest first
        """
        try:
            # Ensure max_results is within API limits
            max_results = min(max(1, int(max_results)), 500)
            
            # Get the list of messages
            result = self.service.users().messages().list(
                userId='me',
                maxResults=max_results,
                q=query if query else ''
            ).execute()

            messages = result.get('messages', [])
            parsed = []

            # Fetch full message details for each message
            for msg in messages:
                txt = self.service.users().messages().get(
                    userId='me', 
                    id=msg['id']
                ).execute()
                parsed_message = self._parse_message(txt=txt, parse_body=False)
                if parsed_message:
                    parsed.append(parsed_message)
                    
            return parsed
            
        except Exception as e:
            logging.error(f"Error reading emails: {str(e)}")
            logging.error(traceback.format_exc())
            raise
        
    def get_email_by_id_with_attachments(self, email_id: str) -> Tuple[dict, dict] | Tuple[None, dict]:
        """
        Fetch and parse a complete email message by its ID including attachment IDs.
        
        Args:
            email_id (str): The Gmail message ID to retrieve
        
        Returns:
            Tuple[dict, list]: Complete parsed email message including body and list of attachment IDs
            Tuple[None, list]: If retrieval or parsing fails, returns None for email and empty list for attachment IDs
        """
        try:
            # Fetch the complete message by ID
            message = self.service.users().messages().get(
                userId='me',
                id=email_id
            ).execute()
            
            # Mark as read (remove UNREAD label) since we're viewing it
            if 'UNREAD' in message.get('labelIds', []):
                try:
                    self.service.users().messages().modify(
                        userId='me', id=email_id,
                        body={'removeLabelIds': ['UNREAD']}
                    ).execute()
                except Exception:
                    pass  # Don't fail the read if marking fails

            # Parse the message with body included
            parsed_email = self._parse_message(txt=message, parse_body=True)

            if parsed_email is None:
                return None, {}

            attachments = {}
            # Check if 'parts' exists in payload before trying to access it
            if "payload" in message and "parts" in message["payload"]:
                for part in message["payload"]["parts"]:
                    if "body" in part and "attachmentId" in part["body"]:
                        attachment_id = part["body"]["attachmentId"]
                        part_id = part["partId"]
                        attachment = {
                            "filename": part["filename"],
                            "mimeType": part["mimeType"],
                            "attachmentId": attachment_id,
                            "partId": part_id
                        }
                        attachments[part_id] = attachment
            else:
                # Handle case when there are no parts (single part message)
                logging.info(f"Email {email_id} does not have 'parts' in payload (likely single part message)")
                if "payload" in message and "body" in message["payload"] and "attachmentId" in message["payload"]["body"]:
                    # Handle potential attachment in single part message
                    attachment_id = message["payload"]["body"]["attachmentId"]
                    attachment = {
                        "filename": message["payload"].get("filename", "attachment"),
                        "mimeType": message["payload"].get("mimeType", "application/octet-stream"),
                        "attachmentId": attachment_id,
                        "partId": "0"
                    }
                    attachments["0"] = attachment

            return parsed_email, attachments
            
        except Exception as e:
            logging.error(f"Error retrieving email {email_id}: {str(e)}")
            logging.error(traceback.format_exc())
            raise
        
    def create_draft(self, to: str, subject: str, body: str, cc: list[str] | None = None) -> dict | None:
        """
        Create a draft email message.
        
        Args:
            to (str): Email address of the recipient
            subject (str): Subject line of the email
            body (str): Body content of the email
            cc (list[str], optional): List of email addresses to CC
            
        Returns:
            dict: Draft message data including the draft ID if successful
            None: If creation fails
        """
        try:
            # Create message body
            message = {
                'to': to,
                'subject': subject,
                'text': body,
            }
            if cc:
                message['cc'] = ','.join(cc)
                
            # Create the message in MIME format (utf-8 avoids 76-char line wrapping)
            mime_message = MIMEText(body, _charset='utf-8')
            mime_message['to'] = to
            mime_message['subject'] = subject
            if cc:
                mime_message['cc'] = ','.join(cc)

            # Encode the message
            raw_message = base64.urlsafe_b64encode(mime_message.as_bytes()).decode('utf-8')
            
            # Create the draft
            draft = self.service.users().drafts().create(
                userId='me',
                body={
                    'message': {
                        'raw': raw_message
                    }
                }
            ).execute()
            
            return draft
            
        except Exception as e:
            logging.error(f"Error creating draft: {str(e)}")
            logging.error(traceback.format_exc())
            raise
        
    def send_email(self, to: str, subject: str, body: str, cc: list[str] | None = None) -> dict | None:
        """
        Send an email message immediately.

        Args:
            to (str): Email address of the recipient
            subject (str): Subject line of the email
            body (str): Body content of the email
            cc (list[str], optional): List of email addresses to CC

        Returns:
            dict: Sent message data if successful
            None: If sending fails
        """
        try:
            mime_message = MIMEText(body, _charset='utf-8')
            mime_message['to'] = to
            mime_message['subject'] = subject
            if cc:
                mime_message['cc'] = ','.join(cc)

            raw_message = base64.urlsafe_b64encode(mime_message.as_bytes()).decode('utf-8')

            result = self.service.users().messages().send(
                userId='me',
                body={'raw': raw_message}
            ).execute()

            return result

        except Exception as e:
            logging.error(f"Error sending email: {str(e)}")
            logging.error(traceback.format_exc())
            raise

    def delete_draft(self, draft_id: str) -> bool:
        """
        Delete a draft email message.
        
        Args:
            draft_id (str): The ID of the draft to delete
            
        Returns:
            bool: True if deletion was successful, False otherwise
        """
        try:
            self.service.users().drafts().delete(
                userId='me',
                id=draft_id
            ).execute()
            return True
            
        except Exception as e:
            logging.error(f"Error deleting draft {draft_id}: {str(e)}")
            logging.error(traceback.format_exc())
            raise
        
    def create_reply(self, original_message: dict, reply_body: str, send: bool = False, cc: list[str] | None = None) -> dict | None:
        """
        Create a reply to an email message and either send it or save as draft.
        
        Args:
            original_message (dict): The original message data (as returned by get_email_by_id)
            reply_body (str): Body content of the reply
            send (bool): If True, sends the reply immediately. If False, saves as draft.
            cc (list[str], optional): List of email addresses to CC
            
        Returns:
            dict: Sent message or draft data if successful
            None: If operation fails
        """
        try:
            to_address = original_message.get('from')
            if not to_address:
                raise ValueError("Could not determine original sender's address")
            
            subject = original_message.get('subject', '')
            if not subject.lower().startswith('re:'):
                subject = f"Re: {subject}"


            mime_message = MIMEText(reply_body, _charset='utf-8')
            mime_message['to'] = to_address
            mime_message['subject'] = subject
            if cc:
                mime_message['cc'] = ','.join(cc)
                
            mime_message['In-Reply-To'] = original_message.get('message_id', original_message.get('id', ''))
            mime_message['References'] = original_message.get('message_id', original_message.get('id', ''))
            
            raw_message = base64.urlsafe_b64encode(mime_message.as_bytes()).decode('utf-8')
            
            message_body = {
                'raw': raw_message,
                'threadId': original_message.get('threadId')  # Ensure it's added to the same thread
            }

            if send:
                # Send the reply immediately
                result = self.service.users().messages().send(
                    userId='me',
                    body=message_body
                ).execute()
            else:
                # Save as draft
                result = self.service.users().drafts().create(
                    userId='me',
                    body={
                        'message': message_body
                    }
                ).execute()
            
            return result
            
        except Exception as e:
            logging.error(f"Error {'sending' if send else 'drafting'} reply: {str(e)}")
            logging.error(traceback.format_exc())
            raise
        
    def get_attachment(self, message_id: str, attachment_id: str) -> dict | None:
        """
        Retrieves a Gmail attachment by its ID.
        
        Args:
            message_id (str): The ID of the Gmail message containing the attachment
            attachment_id (str): The ID of the attachment to retrieve
        
        Returns:
            dict: Attachment data including filename and base64-encoded content
            None: If retrieval fails
        """
        try:
            attachment = self.service.users().messages().attachments().get(
                userId='me',
                messageId=message_id, 
                id=attachment_id
            ).execute()
            return {
                "size": attachment.get("size"),
                "data": attachment.get("data")
            }
            
        except Exception as e:
            logging.error(f"Error retrieving attachment {attachment_id} from message {message_id}: {str(e)}")
            logging.error(traceback.format_exc())
            raise

    def get_thread(self, thread_id: str, user_id: str) -> dict | None:
        """
        Fetch all messages in a thread by thread ID.

        Args:
            thread_id: The Gmail thread ID
            user_id: The email address of the current account (used for direction detection)

        Returns:
            dict with threadId, message_count, messages list (each with direction),
            last_message_from, last_message_direction, last_message_date
            None if retrieval fails
        """
        try:
            thread = self.service.users().threads().get(
                userId='me',
                id=thread_id,
                format='full'
            ).execute()

            # Mark all unread messages in thread as read since we're viewing them
            unread_ids = [
                msg['id'] for msg in thread.get('messages', [])
                if 'UNREAD' in msg.get('labelIds', [])
            ]
            for uid in unread_ids:
                try:
                    self.service.users().messages().modify(
                        userId='me', id=uid,
                        body={'removeLabelIds': ['UNREAD']}
                    ).execute()
                except Exception:
                    pass  # Don't fail the read if marking fails

            messages = []
            user_id_lower = user_id.lower()

            for msg in thread.get('messages', []):
                parsed = self._parse_message(msg, parse_body=True)
                if parsed:
                    # Determine direction based on From address
                    from_addr = parsed.get('from', '').lower()
                    if user_id_lower in from_addr:
                        parsed['direction'] = 'outbound'
                    else:
                        parsed['direction'] = 'inbound'
                    messages.append(parsed)

            if not messages:
                return {'threadId': thread_id, 'message_count': 0, 'messages': []}

            last = messages[-1]
            return {
                'threadId': thread_id,
                'message_count': len(messages),
                'messages': messages,
                'last_message_from': last.get('from'),
                'last_message_direction': last.get('direction'),
                'last_message_date': last.get('date'),
            }

        except Exception as e:
            logging.error(f"Error retrieving thread {thread_id}: {str(e)}")
            logging.error(traceback.format_exc())
            raise

    def modify_labels(self, message_ids: List[str], add_label_ids: List[str] | None = None, remove_label_ids: List[str] | None = None) -> List[dict]:
        """
        Add/remove labels on one or more messages.

        Args:
            message_ids: List of Gmail message IDs to modify
            add_label_ids: Label IDs to add (e.g., ['Label_123', 'STARRED'])
            remove_label_ids: Label IDs to remove (e.g., ['UNREAD', 'INBOX'])

        Returns:
            List of result dicts with id and status for each message
        """
        results = []
        body = {}
        if add_label_ids:
            body['addLabelIds'] = add_label_ids
        if remove_label_ids:
            body['removeLabelIds'] = remove_label_ids

        for msg_id in message_ids:
            try:
                result = self.service.users().messages().modify(
                    userId='me',
                    id=msg_id,
                    body=body
                ).execute()
                results.append({"id": msg_id, "status": "ok", "labelIds": result.get("labelIds", [])})
            except Exception as e:
                logging.error(f"Error modifying labels on {msg_id}: {str(e)}")
                results.append({"id": msg_id, "status": "error", "error": str(e)})

        return results

    def create_label(self, name: str) -> dict | None:
        """
        Create a new Gmail label.

        Args:
            name: Label name (use '/' for nesting, e.g., 'Agent/Processed')

        Returns:
            dict with label id and name, or None on failure
        """
        try:
            label = self.service.users().labels().create(
                userId='me',
                body={
                    'name': name,
                    'labelListVisibility': 'labelShow',
                    'messageListVisibility': 'show'
                }
            ).execute()
            return {"id": label['id'], "name": label['name']}
        except Exception as e:
            logging.error(f"Error creating label '{name}': {str(e)}")
            logging.error(traceback.format_exc())
            raise

    def list_labels(self) -> List[dict]:
        """
        List all labels in the account.

        Returns:
            List of dicts with id, name, and type for each label
        """
        try:
            results = self.service.users().labels().list(userId='me').execute()
            labels = results.get('labels', [])
            return [{"id": l['id'], "name": l['name'], "type": l.get('type', 'user')} for l in labels]
        except Exception as e:
            logging.error(f"Error listing labels: {str(e)}")
            logging.error(traceback.format_exc())
            raise