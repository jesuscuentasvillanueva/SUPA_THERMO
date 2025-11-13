from kivy.app import App
from kivy.uix.label import Label
from kivy.clock import Clock
from jnius import autoclass, PythonJavaClass, java_method


class Runnable(PythonJavaClass):
    __javainterfaces__ = ['java/lang/Runnable']
    __javacontext__ = 'app'

    def __init__(self, func):
        super().__init__()
        self.func = func

    @java_method('()V')
    def run(self):
        self.func()


class ReaderCallback(PythonJavaClass):
    __javainterfaces__ = ['android/nfc/NfcAdapter$ReaderCallback']
    __javacontext__ = 'app'

    def __init__(self, on_tag):
        super().__init__()
        self.on_tag = on_tag
        self.Ndef = autoclass('android.nfc.tech.Ndef')

    @java_method('(Landroid/nfc/Tag;)V')
    def onTagDiscovered(self, tag):
        try:
            uid_arr = tag.getId()
            uid_hex = ''.join(['%02X' % (uid_arr[i] & 0xFF) for i in range(len(uid_arr))])
            text = None
            uri = None
            ndef = self.Ndef.get(tag)
            if ndef:
                try:
                    ndef.connect()
                    msg = ndef.getNdefMessage()
                    if msg:
                        recs = msg.getRecords()
                        for r in recs:
                            t = r.getType()
                            t_b = bytes([(t[i] & 0xFF) for i in range(len(t))])
                            payload = r.getPayload()
                            p_b = bytes([(payload[i] & 0xFF) for i in range(len(payload))])
                            if t_b == b'T' and len(p_b) > 1:
                                lang_len = p_b[0] & 0x3F
                                text = p_b[1+lang_len:].decode('utf-8', 'ignore')
                            elif t_b == b'U' and len(p_b) > 1 and uri is None:
                                uri = p_b[1:].decode('utf-8', 'ignore')
                finally:
                    try:
                        ndef.close()
                    except Exception:
                        pass
            info = {'uid': uid_hex, 'text': text, 'uri': uri}
            Clock.schedule_once(lambda dt: self.on_tag(info))
        except Exception as e:
            Clock.schedule_once(lambda dt: self.on_tag({'error': str(e)}))


class NFCApp(App):
    def build(self):
        self.label = Label(text='Acerque una etiqueta NFC')
        return self.label

    def on_start(self):
        self.enable_reader_mode()

    def on_resume(self):
        self.enable_reader_mode()

    def on_pause(self):
        self.disable_reader_mode()
        return True

    def on_stop(self):
        self.disable_reader_mode()

    def enable_reader_mode(self):
        PythonActivity = autoclass('org.kivy.android.PythonActivity')
        NfcAdapter = autoclass('android.nfc.NfcAdapter')
        activity = PythonActivity.mActivity
        adapter = NfcAdapter.getDefaultAdapter(activity)
        self._adapter = adapter
        if adapter is None:
            self.label.text = 'NFC no disponible'
            return
        flags = (
            NfcAdapter.FLAG_READER_NFC_A
            | NfcAdapter.FLAG_READER_NFC_B
            | NfcAdapter.FLAG_READER_NFC_F
            | NfcAdapter.FLAG_READER_NFC_V
            | NfcAdapter.FLAG_READER_NO_PLATFORM_SOUNDS
        )
        self._callback = ReaderCallback(self.on_tag_read)
        activity.runOnUiThread(Runnable(lambda: adapter.enableReaderMode(activity, self._callback, flags, None)))

    def disable_reader_mode(self):
        try:
            PythonActivity = autoclass('org.kivy.android.PythonActivity')
            activity = PythonActivity.mActivity
            if getattr(self, '_adapter', None):
                adapter = self._adapter
                activity.runOnUiThread(Runnable(lambda: adapter.disableReaderMode(activity)))
        except Exception:
            pass

    def on_tag_read(self, info):
        if 'error' in info:
            self.label.text = 'Error: ' + info['error']
        else:
            s = 'UID: ' + info['uid']
            if info.get('text'):
                s += '\nTEXT: ' + info['text']
            if info.get('uri'):
                s += '\nURI: ' + info['uri']
            self.label.text = s


if __name__ == '__main__':
    NFCApp().run()
