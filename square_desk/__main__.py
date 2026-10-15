import os
import uvicorn


if __name__ == '__main__':
    uvicorn.run('square_desk.app:create_app', factory=True, host='0.0.0.0', port=int(os.getenv('PORT', '8080')),
                workers=1, access_log=False, log_level='info')
