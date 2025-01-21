# logger.py
import logging

class Logger:
    """
    Loggerクラスは、特定の設定でログを処理するためのクラスです。
    属性:
        logger (logging.Logger): ログメッセージを記録するために使用されるロガーインスタンス。
    メソッド:
        __init__():
            Loggerインスタンスを初期化し、ストリームハンドラとフォーマッタを設定します。
        debug(message: str):
            DEBUGレベルのメッセージを記録します。
        info(message: str):
            INFOレベルのメッセージを記録します。
        warning(message: str):
            WARNINGレベルのメッセージを記録します。
        error(message: str):
            ERRORレベルのメッセージを記録します。
    """
    def __init__(self):
        self.logger = logging.getLogger("tem_loggeer")
        self.logger.setLevel(logging.DEBUG)
        
        # 標準出力へのハンドラを追加
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        
        # フォーマットを設定（必要に応じて変更可能）
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        stream_handler.setFormatter(formatter)
        
        # ハンドラをロガーに追加
        self.logger.addHandler(stream_handler)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self,message):
        """エラーレベルのメッセージを記録"""
        self.logger.error(message)

