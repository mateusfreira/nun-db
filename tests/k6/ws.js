import ws from 'k6/ws';
import {
    check
} from 'k6';

import {
    Trend
} from 'k6/metrics';

const watchGage = new Trend('watch', true);

export default function() {
    const url = 'ws://127.0.0.1:3058';
    //const url = 'wss://ws.nundb.org';
    const params = {};
    const keyName = `name_${__VU}_${Date.now()}`;

    const response = ws.connect(url, params, function(socket) {
        socket.on('open', function open() {
            console.log('connected');

            socket.send('auth mateus mateus')
            socket.send('create-db vue vue_pwd');
            //socket.send('use-db vue vue-pwd'); 
            socket.send('use-db vue vue_pwd');
            socket.send(`remove ${keyName}`);
            socket.send(`watch ${keyName}`);
            socket.setInterval(function timeout() {
                socket.send(`set ${keyName} mateus-${Date.now()}`);
                console.log('Pinging every 1sec (setInterval test)');
            }, 1000);
        });

        socket.on('message', (data) => {
            console.log(data);
            if (~data.indexOf('changed')) {
                const now = Date.now();
                const time = data.split('-')[1];
                const spent = now - time;
                watchGage.add(spent);
            }
            console.log('Message received: ', data)
        });


        socket.on('close', () => console.log('disconnected'));

        socket.on('error', (e) => {
            if (e.error() != 'websocket: close sent') {
                console.log('An unexpected error occurred: ', e.error());
            }
        });

        socket.setTimeout(function() {
            console.log('2 seconds passed, closing the socket');
            socket.close();
        }, 20000);
    });

    check(response, {
        'status is 101': (r) => r && r.status === 101
    });
}

export const options = {
    stages: [{
            duration: '5m',
            target: 10
        }, // simulate ramp-up of traffic from 1 to 100 users over 5 minutes.
        {
            duration: '5m',
            target: 100
        }, // stay at 100 users for 10 minutes
        {
            duration: '5m',
            target: 200
        }, // stay at 100 users for 10 minutes
        {
            duration: '5m',
            target: 50
        }, // stay at 100 users for 10 minutes
        {
            duration: '5m',
            target: 0
        }, // ramp-down to 0 users
    ],
    thresholds: {
        'watch': ['p(95)<200'], // 95% of requests must complete below 200ms
    },
};
