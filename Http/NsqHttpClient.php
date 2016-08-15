<?php
/**
 * Nsq Http 客户端
 * @author Janson
 * @create 2016-04-22
 */

namespace App\Helper\Nsq;
use GuzzleHttp\Client;
use App\Helper\Log;

class NsqHttpClient {
	private $_host;
	private $_port;
	private $_topic;
	private $_module;
	private $_nch = null;			//nsq client handle
	private $_retryTimes = 1;		//重试次数
	private $_connectionTimeout = 3;
	private $_readWriteTimeout = 3;	//读写时长  单位：秒

	public $logPath = SERVICE_LOG_PATH . 'nsq/';

	private static $_instances = array();

	private function __construct($module) {
		$this->_module = $module;
		$this->getHttpClient();		//获取http client实例
	}

	/**
	 * 获取nsq client单例
	 * @param string $module	nsq服务模块key
	 * @return mixed
	 * @throws \Exception
	 */
	public static function getInstance($module) {
		try {
			if (empty($module)) {
				throw new \InvalidArgumentException('NsqHttpClient: module key can not be empty');
			}

			if ( ! isset(self::$_instances[$module])) {
				self::$_instances[$module] = new self($module);
			}
		} catch(\Exception $e) {
			Log::error(
				'NsqHttpClient',
				SERVICE_LOG_PATH . 'nsq/error.log',
				$e->getMessage(),
				array(
					'module' => $module,
					'userId' => $userId,
					'corpId' => $corpId
				)
			);

			throw $e;
		}

		return self::$_instances[$module];
	}

	/**
	 * 一次发布单条nsq消息
	 * @param array $nsqData
	 * @return bool
	 */
	public function pub($nsqData) {
		return $this->doPub('pub', $nsqData);
	}

	/**
	 * 一次发布多条nsq消息
	 * @param array $nsqDatas
	 * @return bool
	 */
	public function mpub($nsqDatas) {
		return $this->doPub('mpub', $nsqDatas);
	}

	/**
	 * 执行nsq消息发布
	 * @param string $cmd
	 * @param array $nsqDatas
	 */
	private function doPub($cmd, $nsqDatas) {
		$requestId = uniqid();

		try {
			if (empty($nsqDatas) || ! is_array($nsqDatas)) {
				throw new \InvalidArgumentException('NsqHttpClient: nsq data is empty or not an array');
			}

			$clientInfo = array(
				'module' => $this->_module,
				'userId' => self::$_userId,
				'corpId' => self::$_corpId,
				'host' => $this->_host,
				'port' => $this->_port,
				'topic' => $this->_topic,
				'cmd' => $cmd,
				'nsqDatas' => $nsqDatas
			);

			if ($cmd == 'mpub') {
				foreach ($nsqDatas as $item) {
					$msgs[] = json_encode($item);
				}
				$message = implode("\n", $msgs);
			}
			else {
				$message = json_encode($nsqDatas);
			}

			$start = microtime(true);
			for ($i = 0; $i <= $this->_retryTimes; $i++) {
				try {
					$response = $this->_nch->request(
						'POST',
						"/{$cmd}?topic={$this->_topic}",
						['body' => $message]
					);

					if ($result = ($response->getStatusCode() == 200)) {
						$clientInfo['RequestTime'] = microtime(true) - $start;

						Log::info(
							'NsqHttpClient',
							$this->logPath . 'request.log',
							"RequestId: $requestId",
							$clientInfo
						);

						break;
					}
					else {
						throw new \Exception('post nsq message fail');
					}
				} catch(\Exception $e) {
					if ($i < $this->_retryTimes) {
						Log::error(
							'NsqHttpClient',
							$this->logPath . 'retry.log',
							"RequestId: $requestId, Times: " . ($i + 1) . ", Error: post nsq message fail",
							$clientInfo
						);
					}
					else{
						throw $e;
					}
				}
			}
		} catch(\Exception $e) {
			Log::error(
				'NsqHttpClient',
				$this->logPath . 'error.log',
				"RequestId: $requestId, Error: {$e->getMessage()}",
				$clientInfo
			);

			throw $e;
		}

		return $result;
	}

	/**
	 * 新建GuzzleHttp\Client实例
	 */
	private function getHttpClient() {
		$this->config();	//加载配置

		//初始化一个GuzzleHttp\Client实例
		$this->_nch = new Client([
			'base_uri' => "http://{$this->_host}:{$this->_port}",
			'connect_timeout' => $this->_connectionTimeout,
			'timeout'  => $this->_readWriteTimeout
		]);
	}

	/**
	 * 获取Nsq服务模块配置
	 * @throws \Exception
	 */
	private function config() {
		$nsq_config = \Zend_Registry::get('nsq_config');

		if( ! isset($nsq_config[$this->_module])) {
			throw new \InvalidArgumentException('NsqHttpClient: module key is wrong');
		}

		if(empty($nsq_config[$this->_module]) || ! is_array($nsq_config[$this->_module])) {
			throw new \InvalidArgumentException('NsqHttpClient: Nsq config is empty or not an array');
		}

		if( ! isset($nsq_config[$this->_module]['host']) || ! isset($nsq_config[$this->_module]['http_port']) || ! isset($nsq_config[$this->_module]['topic'])) {
			throw new \InvalidArgumentException('NsqHttpClient: Nsq host, http_port or topic is not set');
		}

		$this->_host = $nsq_config[$this->_module]['host'];
		$this->_port = $nsq_config[$this->_module]['http_port'];
		$this->_topic = $nsq_config[$this->_module]['topic'];

		if(isset($nsq_config[$this->_module]['log_path'])) {
			$this->logPath = $nsq_config[$this->_module]['log_path'];
		}

		if(isset($nsq_config[$this->_module]['connection_timeout'])) {
			$this->_connectionTimeout = intval($nsq_config[$this->_module]['connection_timeout']);
		}

		if(isset($nsq_config[$this->_module]['readwrite_timeout'])) {
			$this->_readWriteTimeout = intval($nsq_config[$this->_module]['readwrite_timeout']);
		}
	}

	/**
	 * 防止克隆
	 */
	private function __clone() {

	}
}