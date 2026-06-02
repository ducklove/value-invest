"""Portfolio condition notifications.

Channel-agnostic alert engine + per-channel senders. v1 ships Telegram;
``channels.dispatch`` fans out to every enabled channel so adding KakaoTalk
later only needs a new sender module + a branch in ``channels.dispatch``.
"""
