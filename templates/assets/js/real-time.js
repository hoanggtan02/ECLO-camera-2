

const RealtimeService = (function () {

    let mqttClient;
    let activeDataTables = {}; 

    function formatMqttMessageForDataTable(topic, payload) {
        const info = payload.info;
        const eventType = topic.split('/').pop();

        if (eventType === 'Rec') {
            return {
                id:          info.RecordID,
                person_name: info.persionName,
                image_path:  `<img src="${info.pic}" alt="Face" class="img-thumbnail" style="width: 60px;">`,
                event_time:  new Date(info.time).toLocaleString('vi-VN'),
                is_no_mask:  parseInt(info.isNoMask) ? '<span class="badge bg-danger">Không KT</span>' : '<span class="badge bg-success">Có KT</span>',
                person_id:   info.personId,
                similarity:  parseFloat(info.similarity1).toFixed(2) + '%',
                person_type: (parseInt(info.PersonType) == 0 ? 'Nhân viên' : 'Người lạ'),
            };
        } 
        else if (eventType === 'Snap') {
            return {
                id:          info.SnapID,
                person_name: '<b>Người lạ (Snap)</b>',
                image_path:  `<img src="${info.pic}" alt="Face" class="img-thumbnail" style="width: 60px;">`,
                event_time:  new Date(info.time).toLocaleString('vi-VN'),
                is_no_mask:  parseInt(info.isNoMask) ? '<span class="badge bg-danger">Không KT</span>' : '<span class="badge bg-success">Có KT</span>',
                person_id:   '',
                similarity:  '',
                person_type: 'Chụp nhanh',
            };
        }
        return null;
    }

    function updateDataTable(topic, data) {
        const eventType = topic.split('/').pop().toLowerCase();
        if (activeDataTables[eventType] && activeDataTables[eventType].instance) {
            const instance = activeDataTables[eventType].instance;
            const newNode = instance.row.add(data).draw(false).node();
            $(newNode).addClass('table-success').delay(5000).queue(function(next) {
                $(this).removeClass('table-success');
                next();
            });
        }
    }

    return {

        init: function () {
            if (mqttClient && mqttClient.connected) {
                return;
            }
            
            console.log('[RealtimeService] Đang kết nối tới MQTT Broker...');
            mqttClient = mqtt.connect('wss://mqtt.ellm.io/mqtt', {
                clientId: 'web-client-' + Math.random().toString(16).substr(2, 8),
                username: 'eclo',
                password: 'Eclo@123'
            });

            mqttClient.on('connect', () => {
                console.log('[RealtimeService] ✅ Kết nối MQTT thành công!');
                const topics = ['mqtt/face/1018656/Rec', 'mqtt/face/1018656/Snap'];
                mqttClient.subscribe(topics, (err) => {
                    if (err) {
                        console.error('[RealtimeService] Lỗi đăng ký topic:', err);
                    } else {
                        console.log('[RealtimeService] Đã đăng ký các topic:', topics.join(', '));
                    }
                });
            });

            mqttClient.on('message', (topic, message) => {
                try {
                    const payload = JSON.parse(message.toString());
                    const formattedData = formatMqttMessageForDataTable(topic, payload);
                    if (formattedData) {
                        updateDataTable(topic, formattedData);
                    }
                } catch (e) {
                    console.error('[RealtimeService] Lỗi xử lý message:', e);
                }
            });

            mqttClient.on('error', (err) => {
                console.error('[RealtimeService] Lỗi kết nối MQTT:', err);
            });
        },

        /**
         * Đăng ký một DataTable để lắng nghe sự kiện.
         * Sẽ được gọi từ main.js mỗi khi một bảng được khởi tạo.
         * @param {string} eventType - Tên sự kiện (ví dụ: 'rec', 'snap')
         * @param {object} instance - Instance của DataTable
         */
        registerTable: function (eventType, instance) {
            if (eventType && instance) {
                activeDataTables[eventType] = { instance: instance };
                console.log(`[RealtimeService] DataTable cho event '${eventType}' đã được đăng ký.`);
            }
        },

        clearRegistrations: function () {
            Object.keys(activeDataTables).forEach(key => delete activeDataTables[key]);
            console.log('[RealtimeService] Tất cả đăng ký DataTable đã được dọn dẹp.');
        }
    };

})(); 