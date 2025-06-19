(function(global) {
	var client;
	var ENCODING = 'utf-16le';
	const active = $('.page-content').attr("data-active");
	const avatar = $('.page-content').attr("data-images");
	var intervalId = null;
	var pendingTimeoutId = null;
	let isTypesetNeeded = false;
	startWebsocket();
	function startWebsocket() {
	    const ws = 'wss://wsa.ellm.io/';
	    client = new WebSocket(ws, [active]);
	    client.binaryType = 'arraybuffer';
	    client.onmessage = function(event) {
	        if (event.data instanceof ArrayBuffer) {
	            var string = new TextDecoder().decode(event.data);
	            var decrypted = encrypt(string, 'd');
	            var decrypted = JSON.parse(decrypted);
                if (isObject(decrypted)) {
                    decrypted = decrypted;
                } 
                else if (isJson(decrypted)) {
                    decrypted = JSON.parse(decrypted);
                } 
                else {
                    var decrypted = decrypted.replace(/\\/g, ""); // Loại bỏ ký tự escape
                    if (isJson(decrypted)) {
                        decrypted = JSON.parse(decrypted); 
                    }
                }
	            var datas = decrypted;
	            console.log(datas);
                if(datas.type=='write' || datas.type=='nextWrite'){
                	write(datas);
                }
                else if(datas.type=='chat'){
                    messages(datas);
                }
                else if(datas.type=='txt2img' || datas.type=='img2img' || datas.type=='inpaint' || datas.type=='removebg'){
                    images(datas);
                }
                else if(datas.type=='song' || datas.type=='cover' || datas.type=='voices' || datas.type=='clone'){
                    audio(datas);
                }
                else if(datas.type=='ai-prompt-images'){
                    prompt_images(datas);
                }
                else if(datas.type=='ai-prompt-audio'){
                    prompt_audio(datas);
                }
                else if(datas.type=='social-like'){
                    social_like(datas);
                }
                else if(datas.type=='social-comments'){
                    social_comment(datas);
                }
	        }
	    };
	    client.onopen = function(event) {
	        // var data = encrypt(JSON.stringify({"sender": active, "type": "ping"}), 'e');
	        // client.send(StringToArrayBuffer(data));
	    };
	    client.onclose = function(event) {
	        setTimeout(startWebsocket, 1000);
	    };
	}
	function isObject(obj) {
        return obj !== null && typeof obj === 'object' && !Array.isArray(obj);
    }
    function isJson(str) {
        try {
            JSON.parse(str);
            return true; 
        } catch (e) {
            return false;
        }
    }
	function send(content) {
        if (client.readyState === WebSocket.OPEN) {
            var Setcontent = JSON.stringify(content);
            var data = encrypt(Setcontent, 'e');
            client.send(StringToArrayBuffer(data));
        } else {
        	swal_error('Error Connection');
        }
    }
	function makeid(length) {
	    let result = '';
	    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
	    const charactersLength = characters.length;
	    for (let i = 0; i < length; i++) {
	        result += characters.charAt(Math.floor(Math.random() * charactersLength));
	    }
	    return result;
	}
	function encrypt(getdata, type) {
	    const { SHA256, enc, AES } = CryptoJS;
	    const secret_iv = type === 'e' ? makeid(16) : getdata.substr(0, 16);
	    const key = SHA256(active).toString(enc.Hex).substr(0, 32);
	    const iv = SHA256(secret_iv).toString(enc.Hex).substr(0, 16);
	    const crypted = type === 'e' ? secret_iv + AES.encrypt(getdata, enc.Utf8.parse(key), { iv: enc.Utf8.parse(iv) }).toString() : AES.decrypt(getdata.slice(16), enc.Utf8.parse(key), { iv: enc.Utf8.parse(iv) }).toString(enc.Utf8);
	    return crypted;
	}
	function uintToString(uintArray) {
	    var encodedString = String.fromCharCode.apply(null, uintArray);
	    return decodeURIComponent(escape(atob(encodedString)));
	}
	function ArrayBufferToString(buffer) {
	    return BinaryToString(String.fromCharCode.apply(null, new Uint8Array(buffer)));
	}
	function StringToArrayBuffer(string) {
	    return new Uint8Array(StringToUint8Array(string)).buffer;
	}
	function BinaryToString(binary) {
	    try {
	        return decodeURIComponent(escape(binary));
	    } catch (error) {
	        if (error instanceof URIError) {
	            return binary;
	        } else {
	            throw error;
	        }
	    }
	}
	function StringToBinary(string) {
	    var chars = [];
	    for (var i = 0; i < string.length; i++) {
	        chars.push(string.charCodeAt(i) & 0xFF);
	    }
	    return String.fromCharCode.apply(null, new Uint8Array(chars));
	}
	function StringToUint8Array(string) {
	    var chars = [];
	    for (var i = 0; i < string.length; i++) {
	        chars.push(string.charCodeAt(i));
	    }
	    return new Uint8Array(chars);
	}
	function typeText() {
	    // var element = $($html);
	    // var htmlContent = element.html();

	    if (window.Worker) {
	        var worker = new Worker('/templates/assets/js/worker.js');
	        // worker.postMessage({ content: content, delay: delay });

	        worker.onmessage = function(e) {
	            // if (e.data.char) {
	            //     // Cập nhật nội dung HTML
	            //     htmlContent += e.data.char;
	            //     element.html(htmlContent);

	            //     // Nếu có vùng cuộn, cập nhật vị trí cuộn
	            //     if (scroll) {
	            //         var scrollBody = element.closest(scroll);
	            //         var previousScrollTop = scrollBody.scrollTop(); // Lưu vị trí cuộn hiện tại

	            //         if (previousScrollTop + scrollBody.innerHeight() < scrollBody[0].scrollHeight) {
	            //             scrollBody.scrollTop(previousScrollTop); // Giữ vị trí cuộn hiện tại
	            //         }
	            //     }
	            // } else if (e.data.done) {
	            //     worker.terminate(); // Kết thúc worker khi hoàn thành
	                MathJax.typesetPromise();
	                Prism.highlightAll();
	            // }
	        };
	    }
	}
	function social_like(data){
		var $this = $('body').find(".page-socials");
		var html = $this.find(".social-item[data-active='"+data.router+"']");
		if(data.code=='send'){
			$this.find(".spinner-load").show();
		}
		else{
			if(data.status=='error'){
				swal_error(data.data.content);
				topbar.hide();
	            $this.find("button").removeAttr('disabled', 'disabled');
			}
			else {
				topbar.hide();
	            $this.find("button").removeAttr('disabled', 'disabled');
	            html.find(".social-like").text(data.data.like);
	            if(data.data.content=='unlike'){
					html.find(".btn-social-like i").removeClass('text-danger');
					html.find(".btn-social-like i").removeClass('ti-heart-filled');
	            	html.find(".btn-social-like i").addClass('ti-heart');
	            }
	            else {
	            	html.find(".btn-social-like i").addClass('text-danger');
					html.find(".btn-social-like i").addClass('ti-heart-filled');
	            	html.find(".btn-social-like i").removeClass('ti-heart');
	            }
	            
			}
			
		}
	}
	function social_comment(data){
		var $this = $('body').find(".page-socials");
		var html = $this.find(".social-item[data-active='"+data.router+"']");
		if(data.code=='send'){
			$this.find(".spinner-load").show();
		}
		else{
			if(data.status=='error'){
				swal_error(data.data.content);
				topbar.hide();
	            $this.find("button").removeAttr('disabled', 'disabled');
			}
			else {
				topbar.hide();
	            $this.find("button").removeAttr('disabled', 'disabled');
	            html.find(".social-comment").text(data.data.comment);
	            var newImages = $('<div class="d-flex social-comment-item align-items-top mb-3">'+
	                    		'<div class="me-2">'+
	                    			'<img data-src="/'+data.data.avatar+'?type=thumb" alt="'+data.data.account+'" class="rounded-circle lazyload" style="width: 40px;">'+
	                    		'</div>'+
	                    		'<div class="d-flex flex-column">'+
	                    			'<div class="bg-body-tertiary rounded-4 p-2"><div class="fw-bold">'+data.data.account+'</div><div class="py-1">'+data.data.content+'</div></div><small class="text-muted">'+data.data.date+'</small>'+
	                    		'</div>'+
	                    	'</div>');
	    		html.find(".social-comments-list").prepend(newImages);
	    		html.find('.social-comment-content').val('');
			}
			
		}
	}
	function write(data){
		var $this = $('body').find(".page-write");
		var html = $this.find(".content-result");
		if(data.code=='send'){
			$this.find(".spinner-load").show();
		}
		else{
			if(data.status=='error'){
				swal_error(data.data.content);
				$this.find(".spinner-load").hide();
				topbar.hide();
	            $this.find("button").removeAttr('disabled', 'disabled');
			}
			else {
				if(data.completed!='DONE'){
					$this.find(".spinner-load").hide();
					topbar.hide();
		            $this.find("button").removeAttr('disabled', 'disabled');
		            // $this.find(".result-button").show();
		            // $this.find(".result-button button").attr("data-router",data.data.active);
		            // $this.find(".result-button a").attr("href",'/users/content/'+data.data.active);
		            html.append(data.data.content);
				}
				else {
					var getcontent = html.html();
		            var updatedContent = processMessageContent(getcontent);
		            html.html(updatedContent); // Update the content-result with processed content
				    setTimeout(() => {
				        Prism.highlightAll();
				    }, 100);
				}
			}
			
		}
	}
	function audio(data){
		var $this = $('body').find(".page-audio");
		var html = $this.find(".content-result");
		if(data.code=='send'){
			$this.find(".spinner-load").show();
		}
		else{
			if(data.status=='error'){
				swal_error(data.data.content);
				$this.find(".spinner-load").hide();
				topbar.hide();
	            $this.find("button").removeAttr('disabled', 'disabled');
			}
			else {
				$this.find(".spinner-load").hide();
				topbar.hide();
	            $this.find("button").removeAttr('disabled', 'disabled');
	            $.each(data.data, function(index, value) {
	            	var newImages = $('<div class="border bg-opacity-10 d-flex text-start shadow align-items-center justify-content-start p-0 position-relative rounded-pill w-100 mb-3">'+
                                    '<div class="audio-item btn w-100 p-2 rounded-pill w-100 text-start "  data-audio="'+value.audio+'">'+
                                    	'<div class="d-flex justify-content-between align-items-center">'+
                                        	'<img data-src="'+value.images+'?type=thumb" class=" w-50px rounded-circle shadow lazyload audio-images">'+
                                            '<div class="w-100 position-relative ms-3">'+
                                                '<strong class="title">'+value.name+'</strong>'+
                                            '</div>'+
                                        '</div>'+
                                    '</div>'+
                                    '<div class="position-absolute end-0">'+
                                        '<div class="d-flex justify-content-end me-3 align-items-center">'+
                                            '<div class="d-flex justify-content-end">'+
                                                '<div class="dropdown me-2">'+
                                                  '<button class="btn btn-light w-50px h-50px rounded-circle d-flex justify-content-center align-items-center" type="button" data-bs-toggle="dropdown" aria-expanded="false">'+
                                                    '<i class="ti ti-share me-2"></i>'+
                                                  '</button>'+
                                                  '<ul class="dropdown-menu rounded-4 bg-body border-0 shadow">'+
                                                    '<li><button class="dropdown-item" data-click="modal" data-multi="share-social" data-url="/social/create-data/audio-song/'+value.active+'"><i class="ti ti-social me-2"></i>Chia sẽ cộng đồng</button></li>'+
                                                    '<li><button class="dropdown-item"><i class="ti ti-share me-2"></i>Chia sẽ ngoài</button></li>'+
                                                  '</ul>'+
                                                '</div>'+
                                                '<a href="/audio/download/'+value.active+'" download="'+value.name+'.mp3" class="btn btn-light w-50px h-50px rounded-circle d-flex justify-content-center align-items-center"><i class="ti ti-download fs-5"></i></a>'+
                                            '</div>'+
                                        '</div>'+
                                    '</div>'+
                                '</div>');
               		// var newImages = $('<div class="btn item-music p-2 rounded-pill border mb-3 d-flex justify-content-start align-items-center" data-img="'+value.images+'" data-url="'+value.audio+'">'+
                    //             '<img src="'+value.images+'" class="w-50px rounded-circle">'+
                    //             '<div class="ms-3 text-start d-flex justify-content-between align-items-center w-100">'+
                    //                 '<div>'+
                    //                     '<strong class="d-block">'+value.name+'</strong>'+
                    //                 '</div>'+
                    //                 '<div class="d-flex">'+
                    //                 	'<div class="dropdown me-1">'+
					// 					  '<button class="btn btn-light w-50px h-50px rounded-circle d-flex justify-content-center align-items-center" type="button" data-bs-toggle="dropdown" aria-expanded="false">'+
					// 					    '<i class="ti ti-share me-2"></i>'+
					// 					  '</button>'+
					// 					  '<ul class="dropdown-menu rounded-4 bg-body border-0 shadow">'+
					// 					    '<li><button class="dropdown-item" data-click="modal" data-multi="share-social" data-url="/social/create-data/audio-song/'+value.active+'"><i class="ti ti-social me-2"></i>Chia sẽ cộng đồng</button></li>'+
					// 					    '<li><button class="dropdown-item"><i class="ti ti-share me-2"></i>Chia sẽ ngoài</button></li>'+
					// 					  '</ul>'+
					// 					'</div>'+
                    //                     '<a href="/audio/download/'+value.active+'" download="'+value.name+'.mp3" class="btn btn-light w-50px h-50px rounded-circle d-flex justify-content-center align-items-center"><i class="ti ti-download fs-5"></i></a>'+
                    //                 '</div>'+
                    //             '</div>'+
                    //         '</div>');
		    		html.prepend(newImages);
		        });
				stopaudio();
				playaudio();
			}
			
		}
	}
	function messages(data){
		var $this = $('body').find(".page-messages");
		if(data.code=='send'){
			checkmessages(data.character, data.data);
		}
		else {
			receivemessages(data.type,data.character,data.data,data.sender,data.completed);
		}
	}
	function images(data){
		var $this = $('body').find(".page-images");
		if(data.code=='send'){
			var newImages = $('<div class="col-lg-3 col-6 items-images ">'+
                    '<div class="card card-hover shadow rounded-4">'+
                       '<div class="card-body p-2">'+
                            '<div class="row g-0 content-result h-200px">'+
                            	'<div class="col-12 h-100 text-center">'+
                                    '<div class="rounded-4 h-100 d-flex justify-content-center align-items-center"><img src="/templates/assets/img/logo-fade.svg" class="w-25"></div>'+
                               '</div>'+
                            '<div>'+
                        '<div>'+
                    '<div>');
		    $this.find(".items-images-list").prepend(newImages);
		}
		else {
			if(data.status=='error'){
				swal_error(data.data.content);
				topbar.hide();
	            $this.find("button").removeAttr('disabled', 'disabled');
	            $this.find(".items-images-list .items-images:first").remove();
			}
			else {
				$this.find(".items-images-list .items-images:first .content-result").html('<div class="col-12">'+
                                    '<div class="position-relative  d-flex justify-content-center align-items-center">'+
                                        '<div class="position-absolute d-flex top-0 justify-content-center align-items-center h-200px items-images-button">'+
                                            '<button class="btn shadow-lg btn-sm btn-light rounded-circle mx-1 h-30px w-30px d-flex justify-content-center align-items-center" data-click="modal" data-url="/users/content/views/'+data.data.active+'">'+
                                                '<i class="ti ti-eye fs-5"></i>'+
                                            '</button>'+
                                            '<a class="btn shadow-lg btn-sm btn-light rounded-circle mx-1 h-30px w-30px d-flex justify-content-center align-items-center" href="'+data.data.images+'" download>'+
                                                '<i class="ti ti-download fs-5"></i>'+
                                            '</a>'+
                                            '<button class="btn shadow-lg btn-sm btn-light rounded-circle mx-1 h-30px w-30px d-flex justify-content-center align-items-center" data-click="modal" data-url="/users/content/delete/'+data.data.active+'">'+
                                                '<i class="ti ti-trash fs-5"></i>'+
                                            '</button>'+
                                        '</div>'+
                                    '</div>'+
                                    '<div class="item-image rounded-4 lazyload" data-size="200" data-bgset="'+data.data.images+'"></div>'+
                                    '<a href="#" class="stretched-link d-lg-none"  data-click="modal" data-url="/users/content/views/'+data.data.active+'"></a>'+
                               '</div>');
				topbar.hide();
	            $this.find("button").removeAttr('disabled', 'disabled');
			}
		}
	}
	function prompt_images(data){
		var $this = $('body').find(".page-images");
		if(data.code=='send'){
			$this.find("#prompt").attr("disabled",'disabled');
		}
		else {
			if(data.status=='error'){
				swal_error(data.data.content);
				topbar.hide();
	            $this.find("button").removeAttr('disabled', 'disabled');
				$this.find("#prompt").removeAttr("disabled",'disabled');
			}
			else {
				topbar.hide();
	            $this.find("button").removeAttr('disabled', 'disabled');
				$this.find("#prompt").removeAttr("disabled",'disabled');
				$this.find("#prompt").val(data.data.prompt);
			}
		}
	}
	function prompt_audio(data){
		var $this = $('body').find(".page-audio");
		if(data.code=='send'){
			$this.find("#prompt").attr("disabled",'disabled');
			$this.find("#title").attr("disabled",'disabled');
			$this.find("#style").attr("disabled",'disabled');
		}
		else {
			if(data.status=='error'){
				swal_error(data.data.content);
				topbar.hide();
	            $this.find("button").removeAttr('disabled', 'disabled');
				$this.find("#prompt").removeAttr("disabled",'disabled');
				$this.find("#title").removeAttr("disabled",'disabled');
				$this.find("#style").removeAttr("disabled",'disabled');
			}
			else {
				topbar.hide();
	            $this.find("button").removeAttr('disabled', 'disabled');
				$this.find("#prompt").removeAttr("disabled",'disabled');
				$this.find("#title").removeAttr("disabled",'disabled');
				$this.find("#style").removeAttr("disabled",'disabled');
				$this.find("#prompt").val(data.data.lyric);
				$this.find("#title").val(data.data.title);
				$this.find("#style").val(data.data.style);
			}
		}
	}
	// function receivemessages(type, character, data, sender,completed) {
	//     var getcharacter = $("body").find('.page-messages[data-active="' + character + '"]');
	//     if (getcharacter.length > 0) {
	//         var messages_list = getcharacter.find(".messages-body");
	//         var existingMessage = messages_list.find('.messages-content-item[data-time="' + data.date + '"]');

	//         if (existingMessage.length > 0) {
	//             existingMessage.find('.content-result').append(data.content);
	//         } else {
	//             var newMessage = $('<div class="d-flex align-items-start my-2 messages-content-item" data-time=' + data.date + '>' +
	//                 '<img data-src="' + data.avatar + '?type=thumb" class="w-30px rounded-circle rounded-3 me-2 lazyload">' +
	//                 '<div class="d-inline-block w-max-100 content-result">' + data.content + '</div>' +
	//                 '</div>');
	//             messages_list.append(newMessage);
	//         }
	//         var messages_body = getcharacter.find(".scroll-vh-100-y");
	// 		var isScrolledToBottom = Math.abs(messages_body.scrollTop() + messages_body.innerHeight() - messages_body[0].scrollHeight) < 1;
	//         if (isScrolledToBottom) {
	// 		    setTimeout(function() {
	// 		        messages_body.animate({
	// 		            scrollTop: messages_body[0].scrollHeight
	// 		        }, 300);
	// 		    }, 100);
	// 		}
	//         getcharacter.find('.messages-content-input').focus();
	//         if(completed=='DONE'){
	//         	MathJax.typesetPromise();
	//         }
	//         typing('messages-typing', character, { "data": data.typing }, active, 'bot');
	//     }
	// }
	function receivemessages(type, character, data, sender, completed) {
	    var getcharacter = $("body").find('.page-messages[data-active="' + character + '"]');
	    if (getcharacter.length > 0) {
	        var messages_list = getcharacter.find(".messages-body");
	        var existingMessage = messages_list.find('.messages-content-item[data-time="' + data.date + '"]');

	        if (existingMessage.length > 0) {
	            existingMessage.find('.content-result').append((data.content));
	        } else {
	            var newMessage = $('<div class="d-flex align-items-start my-2 messages-content-item" data-time=' + data.date + '>' +
	                '<img data-src="' + data.avatar + '?type=thumb" class="w-30px rounded-circle rounded-3 me-2 lazyload">' +
	                '<div class="d-inline-block w-max-90 content-result">' + (data.content) + '</div>' +
	                '</div>');
	            messages_list.append(newMessage);
	        }
	        
	        getcharacter.find('.messages-content-input').focus();
	        if (completed == 'DONE') {
	            MathJax.typesetPromise().then(() => {
	                // Optional: If you want to do something after MathJax typesetting is complete
	            });
	            var getcontent = existingMessage.find('.content-result').html();
	            var updatedContent = processMessageContent(getcontent);
	            existingMessage.find('.content-result').html(updatedContent); // Update the content-result with processed content
			    setTimeout(() => {
			        Prism.highlightAll();
			    }, 100);
	        }
	        typing('messages-typing', character, { "data": data.typing }, active, 'bot');
	    }
	}
	function processMessageContent(content) {
	    const codeBlockRegex = /```([^\s]+)\n([\s\S]*?)```/g;
	    return formattedContent = content.replace(codeBlockRegex, function (match, language, code) {
	        const langClass = language ? `language-${language}` : 'language-default';
	        return '<pre class="'+langClass.replace(/<br\s*\/?>/g, '')+'"><code class="'+langClass.replace(/<br\s*\/?>/g, '')+'">'+code.trim()+'</code></pre>';
	    });
	    return formattedContent;
	}
	// function receivemessages(type, character, data, sender) {
	//     var getcharacter = $("body").find('.page-messages[data-active="' + character + '"]');
	//     if (getcharacter.length > 0) {
	//         var messages_list = getcharacter.find(".messages-body");
	//         var content = '';
	//         var content_send = data.content_send || '';
	//         if (data.type === 'images' || data.type === 'video') {
	//             content = '<button class="btn border-0 p-0 modal-url" data-url="/action/views-cards/' + data.photo + '/"><' + (data.type === 'images' ? 'img' : 'video') + ' src="' + data.url + '" class="rounded-3 w-200px"' + (data.type === 'video' ? ' controls' : '') + '></button><div class="mt-1">' + data.content + '</div>';
	//         } else {
	//             content = '<div>' + data.content + '</div>';
	//         }
	//         if(data.voice=='true'){
	//         	voice = '<button data-active="' + data.contents + '" class="btn-voice-ready btn btn-pink border border-light rounded-circle h-30px w-30px d-flex justify-content-center align-items-center position-absolute" style="right:-10px;bottom:-10px"><i class="fa-solid fa-volume-high text-light"></i></button>';
	//         }
	//         var newMessage = $('<div class="d-flex align-items-start my-2 messages-content-item" data-time='+data.time+'>'+
	// 	        '<img data-src="'+data.avatar+'?type=thumb" class="w-30px rounded-circle rounded-3 me-2 lazyload">'+
	// 	        '<div class="d-inline-block w-max-100 content-result"></div>'+
	// 	    '</div>');
	// 	    messages_list.append(newMessage);

	// 	    typeText(newMessage.find('.content-result'), data.content, 2,".scroll-vh-100-y");
	//         var messages_body = getcharacter.find(".scroll-vh-100-y");
	//         messages_body.animate({
	//             scrollTop: messages_body.get(0).scrollHeight
	//         }, 0);
	//         typing('messages-typing', character, { "data": data.typing }, active, 'bot');
	//         // prepend_receiver(character, data.content, data);
	//     }
	// }
	function sendmessages(character, data, check = null) {
	    var getcharacter = $("body").find('.page-messages[data-active="' + character + '"]');
	    var messages_list = getcharacter.find(".messages-body");
	    if (getcharacter.length > 0) {
	        var checked = (check === 'true') ? '<i class="ti ti-checks fs-6"></i>':'<div class="spinner-border w-15px h-15px" role="status"><span class="visually-hidden">Loading...</span></div>';
	        messages_list.append('<div class="d-flex justify-content-end align-items-start my-3 messages-content-item">'+
									'<div class="position-relative">'+
										'<div class="me-1 spinner-load d-block text-success">' + checked + '</div>'+
									'</div>'+
									'<div class="bg-danger-subtle text-body p-2 rounded-4 d-inline-block w-max-90">'
										+ data.content +
									'</div>'+
									'<img data-src="/'+avatar+'?type=thumb" class="h-30px w-30px rounded-circle ms-2 lazyload">'+
								'</div>');
	        var messages_body = $("body");
	        messages_body.animate({
	            scrollTop: messages_body[0].scrollHeight
	        }, 100); // Thời gian cuộn (300ms)
	        $('.messages-content-item').removeClass('new-message');
	    }
	}
	function checkmessages(character, data) {
	    var getcharacter = $("body").find('.page-messages[data-active="' + character + '"]');
	    var messages_list = getcharacter.find(".messages-body");
	    if (getcharacter.length > 0) {
	        var spinner = messages_list.find(".messages-content-item .spinner-load");
	        if (spinner.find('.spinner-border').length === 0) {
	            sendmessages(character, data, 'true');
	        } else {
	            spinner.html('<i class="ti ti-checks fs-6"></i>');
	        }
	        typing('messages-typing', character, { "data": 'typing' }, active, 'bot');
	    }
	}
	function typing(type, character, data, sender, bot = false) {
		var getcharacter = $("body").find('.page-messages[data-active="' + character + '"]');
	    var typing = getcharacter.find(".messages-content-input-typing");
	    if ((sender !== active || bot) && data.data === 'typing') {
	        typing.show();
	    } else {
	        typing.hide();
	        $('.page-messages .messages-content-button').removeAttr("disabled");
	    	$('.page-messages .messages-content-input').removeAttr("disabled");
	    }
	}
	$(document).on('keypress click', '.messages-content-input, .messages-content-button', function(e) {
	    if ((e.type === 'keypress' && e.which === 13 && !e.shiftKey) || (e.type === 'click' && $(this).hasClass('messages-content-button'))) {
	    	$('.page-messages .messages-content-button').attr("disabled","disabled");
	    	$('.page-messages .messages-content-input').attr("disabled","disabled");
	        var input = $("body").find('.page-messages');
	        var character = input.attr("data-active");
	        var content = input.find(".messages-content-input").val();
	        var lang = input.find(".messages-lang").val();
	        var style = input.find(".messages-style").val();
	        if (content !== '') {
	            var data = {
	                "content": content,
	                "lang": lang,
	                "style": style,
	                "date": 'Just Now',
	                "data": 'text',
	            };
	            sendmessages(character, data);
	            send({"status": "success", "sender": active, "type": 'chat',"stream":"true", "character": character, "data": data, "code": 'send'});
	            $('.page-messages .messages-content-input').val('');
	            e.preventDefault();
	            return false;
	        } else {
	            swal_error('Not Empty');
		    	$('.page-messages .messages-content-button').removeAttr("disabled");
		    	$('.page-messages .messages-content-input').removeAttr("disabled");
	        }
	    }
	});
	global.send = send;
})(window);