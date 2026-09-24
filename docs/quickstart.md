# Quickstart: 설치부터 데이터 재마운트까지

이 안내는 공개된 **Chart 0.5.10 / Controller 0.4.11 / Node 0.4.7** 조합을 사용한다.
현재 checkout의 Chart version과 별개로, 아래 명령은 이미 배포된 artifact에 고정한다.
0.5.10에서도 `storageClass.defaultClass=false`를 명시해 기존 클러스터의 기본 StorageClass를 유지한다.
Chart 0.6.0부터는 이 값이 기본값이다.

ShiftPV는 기존 node-local directory를 RWO PVC로 제공한다. 이 안내는 최초 설치와 재마운트를 검증한다.
노드 간 이동은 [cold move 운영 절차](../charts/shiftpv/README.md)를 따른다.
복제, backup, 영구적으로 잃은 node/disk의 자동 복구는 제공하지 않는다.

## 1. 준비

- Kubernetes 1.35+, Linux kernel 5.6+와 `helm`, `kubectl`이 필요하다.
- Node DaemonSet과 helper가 privileged/root로 동작할 수 있어야 한다.
- 기존 ShiftPV 설치가 없는 클러스터에서 시작한다. 기존 설치는 [업그레이드 절차](development/versioning.md#existing-installation-upgrades)를 따른다.
- 참여시킬 node에서 운영자가 소유한 writable filesystem directory를 준비한다. Chart는 disk를 format하거나 mount하지 않는다.

대상 클러스터와 참여 node를 확인한다.

```bash
kubectl config current-context
kubectl get nodes -o wide
kubectl get storageclass
```

아래 `worker-a`는 `kubectl get nodes`에 표시된 실제 node 이름으로 바꾼다.
이 예제는 node 하나와 Pool 하나로 시작한다.

```bash
NODE_NAME=worker-a
POOL_PATH=/var/lib/shiftpv
```

**해당 node에 접속해** `/var/lib/shiftpv`를 준비한다. 별도 disk를 쓴다면 먼저 그 filesystem이
원하는 경로에 mount되어 있는지 확인한다. 준비된 filesystem에 최소 1Gi 여유 공간이 있어야 한다.

```bash
sudo install -d -m 0750 /var/lib/shiftpv
findmnt -T /var/lib/shiftpv
df -h /var/lib/shiftpv
```

## 2. 공개 Chart 설치

다시 `kubectl`을 사용하는 터미널에서 실행한다. MicroK8s라면 `KUBELET_ROOT`를
`/var/snap/microk8s/common/var/lib/kubelet`으로 지정한다.

```bash
KUBELET_ROOT=/var/lib/kubelet
helm repo add shiftpv https://project-jelly.github.io/ShiftPV
helm repo update shiftpv
helm install shiftpv shiftpv/shiftpv \
  --namespace shiftpv-system --create-namespace \
  --version 0.5.10 \
  --set storageClass.defaultClass=false \
  --set-string node.kubeletRootDir="${KUBELET_ROOT}" \
  --set-string controller.image.tag='0.4.11@sha256:6ff4c151f0aed6b66d26be58508870a9ea15c58afc20fd4619fb86062b7430ba' \
  --set-string node.image.tag='0.4.7@sha256:f549f313aa2d1aadae0fdae2f95dd04464f11ec38850f9f867b8af64d5dc2e4b' \
  --wait --timeout 8m
kubectl -n shiftpv-system rollout status deployment/shiftpv-controller --timeout=5m
kubectl -n shiftpv-system rollout status daemonset/shiftpv-node --timeout=5m
kubectl get storageclass
```

`shiftpv`, `shiftpv-retain`에는 `(default)` 표시가 없어야 한다.

## 3. Pool 등록

준비한 node와 directory를 등록한다. 여기서 1Gi는 할당 판단에 쓰는 Pool limit이며 filesystem hard quota가 아니다.

```bash
kubectl apply -f - <<EOF
apiVersion: shiftpv.io/v1alpha1
kind: ShiftPVPool
metadata:
  name: quickstart
spec:
  nodeName: ${NODE_NAME}
  mountPath: ${POOL_PATH}
  capacity:
    limit: 1Gi
EOF
kubectl wait --for=condition=Ready shiftpvpool/quickstart --timeout=3m
kubectl get shiftpvpool/quickstart -o yaml
```

`Ready=True`, `status.observedGeneration == metadata.generation`, inventory의 `valid: true`,
`truncated`가 `true`가 아닌지 확인한다 (`false` 필드는 출력에서 생략될 수 있다).
준비되지 않은 Pool은 새 volume을 할당하지 않는다.

## 4. PVC와 Pod 생성

검증용 PVC는 `shiftpv`를 명시한다. **이 class는 PVC 삭제 시 데이터를 삭제한다.**
PVC 삭제 뒤에도 데이터 보존이 필요한 실제 workload는 `shiftpv-retain`을 선택하고
[Retain 회수 절차](../charts/shiftpv/README.md#retain-volume-회수)를 확인한다.

```bash
kubectl create namespace shiftpv-demo
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
  namespace: shiftpv-demo
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: shiftpv
  resources:
    requests:
      storage: 64Mi
EOF
```

`WaitForFirstConsumer` 정책이므로 이때 PVC가 `Pending`인 것은 정상이다. 아래 Pod가 만들어지면 할당된다.
같은 manifest로 다시 mount할 수 있도록 파일로 저장한다.

```bash
cat > shiftpv-demo-pod.yaml <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: writer
  namespace: shiftpv-demo
spec:
  terminationGracePeriodSeconds: 1
  containers:
    - name: shell
      image: busybox:1.37
      command: [sh, -ec, 'sleep 86400']
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: data
EOF
kubectl apply -f shiftpv-demo-pod.yaml
kubectl -n shiftpv-demo wait --for=condition=Ready pod/writer --timeout=5m
kubectl -n shiftpv-demo get pvc,pod -o wide
kubectl -n shiftpv-demo exec writer -- sh -ec 'printf "ShiftPV quickstart\n" > /data/proof; sync'
BEFORE=$(kubectl -n shiftpv-demo exec writer -- sha256sum /data/proof)
printf '%s\n' "${BEFORE}"
```

## 5. 재마운트 확인

Pod만 삭제하고 동일 PVC를 다시 mount한다. Pod 시작 명령은 파일을 쓰지 않으므로 기존 데이터 보존을 검증한다.

```bash
kubectl -n shiftpv-demo delete pod writer --wait=true
kubectl apply -f shiftpv-demo-pod.yaml
kubectl -n shiftpv-demo wait --for=condition=Ready pod/writer --timeout=5m
AFTER=$(kubectl -n shiftpv-demo exec writer -- sha256sum /data/proof)
test "${BEFORE}" = "${AFTER}" && echo 'PASS: data survived remount'
```

이 결과는 해당 환경의 설치·쓰기·재마운트 확인이다. 운영 전에는 대상 filesystem에서의
[장애·재부팅·soak 검증](development/testing.md)을 별도로 수행한다.

## 6. 검증용 데이터 정리

다음 명령은 이 안내에서 만든 **검증 데이터와 PVC를 삭제**한다. 실제 workload에 적용하지 않는다.

```bash
PV_NAME=$(kubectl -n shiftpv-demo get pvc data -o jsonpath='{.spec.volumeName}')
VOLUME_ID=$(kubectl get "pv/${PV_NAME}" -o jsonpath='{.spec.csi.volumeHandle}')
kubectl -n shiftpv-demo delete pod writer --wait=true
kubectl -n shiftpv-demo delete pvc data --wait=true
kubectl wait --for=delete "pv/${PV_NAME}" --timeout=5m
kubectl wait --for=delete "shiftpvvolume/${VOLUME_ID}" --timeout=5m
kubectl delete namespace shiftpv-demo
rm shiftpv-demo-pod.yaml
```

Pool과 driver는 다음 사용을 위해 남는다. 완전히 제거하려면
[제거 절차](../charts/shiftpv/README.md)를 따른다. 삭제가 대기 중이면 로그와 journal을 확인하고 finalizer를 강제로 지우지 않는다.
