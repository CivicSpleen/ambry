from ui import app

import logging
from logging import FileHandler


#file_handler = FileHandler(os.path.join(cache_dir, 'web.log'))
#file_handler.setLevel(logging.WARNING)
#app.logger.addHandler(file_handler)

app.run(host='0.0.0.0', port=8080, debug=True)
